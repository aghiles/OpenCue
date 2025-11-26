
/*
 * Copyright Contributors to the OpenCue Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.imageworks.spcue.dao.redis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import com.imageworks.spcue.grpc.job.FrameState;

import java.util.HashMap;
import java.util.Map;

/**
 * Listens for entity change events and syncs them to Redis.
 * Uses @TransactionalEventListener to ensure sync happens only after
 * the SQL transaction commits successfully.
 */
@Component
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisSchedulingEventListener {

    private static final Logger logger = LogManager.getLogger(RedisSchedulingEventListener.class);

    private final RedisTemplate<String, String> redisTemplate;

    // Redis key prefixes
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";

    public RedisSchedulingEventListener(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        logger.info("Redis scheduling event listener initialized");
    }

    /**
     * Handle frame state changes.
     * - When frame becomes WAITING: add to waiting sorted set
     * - When frame leaves WAITING: remove from waiting sorted set
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onFrameStateChanged(FrameStateChangedEvent event) {
        String frameId = event.getFrameId();
        String layerId = event.getLayerId();
        String jobId = event.getJobId();
        FrameState previousState = event.getPreviousState();
        FrameState newState = event.getNewState();

        logger.debug("Frame state changed: {} from {} to {}", frameId, previousState, newState);

        try {
            String waitingSetKey = FRAMES_WAITING_PREFIX + layerId;

            if (newState == FrameState.WAITING) {
                // Frame became WAITING - add to sorted set with dispatch order as score
                redisTemplate.opsForZSet().add(waitingSetKey, frameId, event.getSortScore());

                // Track that this layer has waiting frames
                redisTemplate.opsForSet().add(LAYERS_WAITING_PREFIX + jobId, layerId);

                logger.debug("Added frame {} to waiting set {} with score {}",
                        frameId, waitingSetKey, event.getSortScore());

            } else if (previousState == FrameState.WAITING) {
                // Frame left WAITING state - remove from sorted set
                redisTemplate.opsForZSet().remove(waitingSetKey, frameId);

                // Check if layer still has waiting frames
                Long waitingCount = redisTemplate.opsForZSet().size(waitingSetKey);
                if (waitingCount == null || waitingCount == 0) {
                    redisTemplate.opsForSet().remove(LAYERS_WAITING_PREFIX + jobId, layerId);
                }

                logger.debug("Removed frame {} from waiting set {}", frameId, waitingSetKey);
            }

            // Update frame metadata hash
            updateFrameMetadata(event);

        } catch (Exception e) {
            logger.error("Failed to sync frame state to Redis: {}", frameId, e);
            // Don't throw - SQL is source of truth, Redis is best-effort cache
        }
    }

    /**
     * Handle layer updates - store layer resource requirements.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onLayerUpdated(LayerUpdatedEvent event) {
        String layerId = event.getLayerId();

        logger.debug("Layer updated: {}", layerId);

        try {
            String layerKey = LAYER_PREFIX + layerId;

            Map<String, String> layerData = new HashMap<>();
            layerData.put("jobId", event.getJobId());
            layerData.put("name", event.getLayerName());
            layerData.put("type", event.getLayerType());
            layerData.put("minCores", String.valueOf(event.getMinCores()));
            layerData.put("maxCores", String.valueOf(event.getMaxCores()));
            layerData.put("minMemory", String.valueOf(event.getMinMemory()));
            layerData.put("minGpus", String.valueOf(event.getMinGpus()));
            layerData.put("maxGpus", String.valueOf(event.getMaxGpus()));
            layerData.put("minGpuMemory", String.valueOf(event.getMinGpuMemory()));
            layerData.put("threadable", String.valueOf(event.isThreadable()));
            layerData.put("tags", event.getTags() != null ? event.getTags() : "");
            layerData.put("command", event.getCommand() != null ? event.getCommand() : "");
            layerData.put("range", event.getRange() != null ? event.getRange() : "");
            layerData.put("chunkSize", String.valueOf(event.getChunkSize()));
            layerData.put("services", event.getServices() != null ? event.getServices() : "");

            redisTemplate.opsForHash().putAll(layerKey, layerData);

            logger.debug("Updated layer metadata in Redis: {}", layerId);

        } catch (Exception e) {
            logger.error("Failed to sync layer to Redis: {}", layerId, e);
        }
    }

    /**
     * Update frame metadata in Redis hash.
     */
    private void updateFrameMetadata(FrameStateChangedEvent event) {
        String frameKey = FRAME_PREFIX + event.getFrameId();

        Map<String, String> frameData = new HashMap<>();
        frameData.put("layerId", event.getLayerId());
        frameData.put("jobId", event.getJobId());
        frameData.put("state", event.getNewState().toString());
        frameData.put("dispatchOrder", String.valueOf(event.getDispatchOrder()));
        frameData.put("layerOrder", String.valueOf(event.getLayerOrder()));

        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }

    /**
     * Handle job completion - clean up Redis data.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onJobCompleted(RedisSchedulingEventPublisher.JobCompletedEvent event) {
        cleanupJob(event.getJobId());
    }

    /**
     * Remove all Redis data for a job (called when job completes/deletes).
     */
    public void cleanupJob(String jobId) {
        try {
            // Get all layers for this job
            String layersWaitingKey = LAYERS_WAITING_PREFIX + jobId;
            var layerIds = redisTemplate.opsForSet().members(layersWaitingKey);

            if (layerIds != null) {
                for (String layerId : layerIds) {
                    // Delete waiting frames set for each layer
                    redisTemplate.delete(FRAMES_WAITING_PREFIX + layerId);
                    // Delete layer metadata
                    redisTemplate.delete(LAYER_PREFIX + layerId);
                }
            }

            // Delete the layers waiting set
            redisTemplate.delete(layersWaitingKey);

            logger.debug("Cleaned up Redis data for job: {}", jobId);

        } catch (Exception e) {
            logger.error("Failed to cleanup Redis data for job: {}", jobId, e);
        }
    }
}


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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import com.imageworks.spcue.grpc.job.FrameState;
import com.imageworks.spcue.grpc.job.JobState;

import java.util.HashMap;
import java.util.Map;

/**
 * Listens for entity change events and syncs them to Redis.
 *
 * Uses @TransactionalEventListener(AFTER_COMMIT) to ensure sync happens
 * only after the SQL transaction commits successfully.
 *
 * Uses @Async to make Redis sync non-blocking - the main thread continues
 * immediately while Redis operations happen in the background.
 *
 * This ensures:
 * 1. SQL is always the source of truth
 * 2. Redis is eventually consistent (after commit)
 * 3. No performance impact on the main dispatch path
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
    private static final String JOB_PREFIX = "job:";
    private static final String JOB_LAYERS_WAITING_PREFIX = "job:layers:waiting:";
    private static final String JOBS_PENDING_PREFIX = "jobs:pending:";

    public RedisSchedulingEventListener(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        logger.info("Redis scheduling event listener initialized");
    }

    /**
     * Handle frame state changes.
     * - When frame becomes WAITING: add to waiting sorted set
     * - When frame leaves WAITING: remove from waiting sorted set
     */
    @Async("redisAsyncExecutor")
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
    @Async("redisAsyncExecutor")
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
        // Additional DispatchFrame fields
        frameData.put("name", event.getFrameName() != null ? event.getFrameName() : "");
        frameData.put("retries", String.valueOf(event.getRetries()));
        frameData.put("version", String.valueOf(event.getVersion()));

        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }

    /**
     * Handle job state/resource changes.
     * - When job becomes PENDING and not paused: add to pending jobs set
     * - When job leaves PENDING or becomes paused: remove from pending jobs set
     */
    @Async("redisAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onJobStateChanged(JobStateChangedEvent event) {
        String jobId = event.getJobId();
        String showId = event.getShowId();
        String facilityId = event.getFacilityId();

        logger.debug("Job state changed: {} state={} paused={}", jobId, event.getState(), event.isPaused());

        try {
            String pendingJobsKey = JOBS_PENDING_PREFIX + showId + ":" + facilityId;
            String jobKey = JOB_PREFIX + jobId;

            if (event.isDispatchable()) {
                // Job is dispatchable - add to pending jobs set and update metadata
                redisTemplate.opsForSet().add(pendingJobsKey, jobId);

                // Update job metadata hash with ALL DispatchFrame fields
                Map<String, String> jobData = new HashMap<>();
                jobData.put("showId", showId);
                jobData.put("facilityId", facilityId);
                jobData.put("folderId", event.getFolderId());
                jobData.put("state", event.getState().toString());
                jobData.put("paused", String.valueOf(event.isPaused()));
                jobData.put("os", event.getOs() != null ? event.getOs() : "");
                jobData.put("priority", String.valueOf(event.getPriority()));
                jobData.put("cores", String.valueOf(event.getCores()));
                jobData.put("minCores", String.valueOf(event.getMinCores()));
                jobData.put("maxCores", String.valueOf(event.getMaxCores()));
                jobData.put("gpus", String.valueOf(event.getGpus()));
                jobData.put("maxGpus", String.valueOf(event.getMaxGpus()));
                jobData.put("tsUpdated", String.valueOf(event.getTsUpdated()));
                jobData.put("folderCores", String.valueOf(event.getFolderCores()));
                jobData.put("folderMaxCores", String.valueOf(event.getFolderMaxCores()));
                jobData.put("folderGpus", String.valueOf(event.getFolderGpus()));
                jobData.put("folderMaxGpus", String.valueOf(event.getFolderMaxGpus()));
                // DispatchFrame fields
                jobData.put("showName", event.getShowName() != null ? event.getShowName() : "");
                jobData.put("jobName", event.getJobName() != null ? event.getJobName() : "");
                jobData.put("shot", event.getShot() != null ? event.getShot() : "");
                jobData.put("owner", event.getOwner() != null ? event.getOwner() : "");
                jobData.put("uid", event.getUid() != null ? String.valueOf(event.getUid()) : "");
                jobData.put("logDir", event.getLogDir() != null ? event.getLogDir() : "");
                jobData.put("lokiURL", event.getLokiURL() != null ? event.getLokiURL() : "");

                redisTemplate.opsForHash().putAll(jobKey, jobData);

                logger.debug("Added job {} to pending set and updated metadata", jobId);

            } else {
                // Job is not dispatchable - remove from pending jobs set
                redisTemplate.opsForSet().remove(pendingJobsKey, jobId);

                // Update state in metadata but keep the data for reference
                redisTemplate.opsForHash().put(jobKey, "state", event.getState().toString());
                redisTemplate.opsForHash().put(jobKey, "paused", String.valueOf(event.isPaused()));

                logger.debug("Removed job {} from pending set", jobId);
            }

        } catch (Exception e) {
            logger.error("Failed to sync job state to Redis: {}", jobId, e);
        }
    }

    /**
     * Handle job completion - clean up Redis data.
     */
    @Async("redisAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onJobCompleted(RedisSchedulingEventPublisher.JobCompletedEvent event) {
        cleanupJob(event.getJobId(), event.getShowId(), event.getFacilityId());
    }

    /**
     * Remove all Redis data for a job (called when job completes/deletes).
     */
    public void cleanupJob(String jobId, String showId, String facilityId) {
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

            // Delete job:layers:waiting set
            redisTemplate.delete(JOB_LAYERS_WAITING_PREFIX + jobId);

            // Remove from pending jobs set
            if (showId != null && facilityId != null) {
                redisTemplate.opsForSet().remove(JOBS_PENDING_PREFIX + showId + ":" + facilityId, jobId);
            }

            // Delete job metadata
            redisTemplate.delete(JOB_PREFIX + jobId);

            logger.debug("Cleaned up Redis data for job: {}", jobId);

        } catch (Exception e) {
            logger.error("Failed to cleanup Redis data for job: {}", jobId, e);
        }
    }
}

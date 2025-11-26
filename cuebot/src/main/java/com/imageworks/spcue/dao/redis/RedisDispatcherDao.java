
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.FrameDao;
import com.imageworks.spcue.grpc.host.ThreadMode;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Redis-based dispatcher DAO for fast frame lookups.
 *
 * Builds DispatchFrame objects entirely from Redis hashes for ZERO SQL on hot path.
 * Falls back to SQL only if Redis data is missing (cache miss).
 *
 * Redis data structures used:
 * - frame:{frameId} - Frame metadata hash
 * - layer:{layerId} - Layer metadata hash
 * - job:{jobId} - Job metadata hash
 */
@Repository
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatcherDao {

    private static final Logger logger = LogManager.getLogger(RedisDispatcherDao.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<List> findDispatchFramesScript;
    private final FrameDao frameDao;

    // Redis key prefixes
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String JOB_PREFIX = "job:";

    public RedisDispatcherDao(RedisTemplate<String, String> redisTemplate,
                               RedisScript<List> findDispatchFramesScript,
                               FrameDao frameDao) {
        this.redisTemplate = redisTemplate;
        this.findDispatchFramesScript = findDispatchFramesScript;
        this.frameDao = frameDao;
        logger.info("Redis dispatcher DAO initialized (full Redis mode)");
    }

    /**
     * Find next dispatch frames for a job using Redis.
     * Builds DispatchFrame entirely from Redis - zero SQL on hot path.
     *
     * @param job   The job to find frames for
     * @param host  The dispatch host with available resources
     * @param limit Maximum number of frames to return
     * @return List of DispatchFrame objects
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit) {
        long startTime = System.currentTimeMillis();

        try {
            // Execute Lua script to find eligible frame IDs
            List<String> frameIds = executeFrameSearch(job, host, limit);

            if (frameIds == null || frameIds.isEmpty()) {
                logger.debug("No eligible frames found in Redis for job {}", job.getJobId());
                return Collections.emptyList();
            }

            // Build DispatchFrame objects from Redis hashes
            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, job.getJobId());

            logger.debug("Redis findNextDispatchFrames: found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search failed, returning empty list", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find next dispatch frames for a job using a VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByProc(job, proc, limit);

            if (frameIds == null || frameIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, job.getJobId());

            logger.debug("Redis findNextDispatchFrames (proc): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by proc failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * Build DispatchFrame objects from Redis hashes.
     * Falls back to SQL for individual frames if Redis data is missing.
     */
    private List<DispatchFrame> buildDispatchFramesFromRedis(List<String> frameIds, String jobId) {
        List<DispatchFrame> frames = new ArrayList<>(frameIds.size());

        // Get job data once (shared across all frames)
        Map<Object, Object> jobData = redisTemplate.opsForHash().entries(JOB_PREFIX + jobId);

        for (String frameId : frameIds) {
            try {
                DispatchFrame frame = buildDispatchFrameFromRedis(frameId, jobData);
                if (frame != null) {
                    frames.add(frame);
                }
            } catch (Exception e) {
                logger.debug("Failed to build frame {} from Redis, trying SQL fallback: {}",
                        frameId, e.getMessage());
                // SQL fallback for cache miss
                try {
                    DispatchFrame frame = frameDao.getDispatchFrame(frameId);
                    if (frame != null) {
                        frames.add(frame);
                    }
                } catch (Exception sqlEx) {
                    logger.debug("SQL fallback also failed for frame {}: {}", frameId, sqlEx.getMessage());
                }
            }
        }

        return frames;
    }

    /**
     * Build a single DispatchFrame from Redis hashes.
     * Returns null if essential data is missing.
     */
    private DispatchFrame buildDispatchFrameFromRedis(String frameId, Map<Object, Object> jobData) {
        // Get frame data
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(FRAME_PREFIX + frameId);
        if (frameData == null || frameData.isEmpty()) {
            throw new RuntimeException("Frame data not found in Redis: " + frameId);
        }

        String layerId = getString(frameData, "layerId");
        if (layerId == null) {
            throw new RuntimeException("Layer ID not found in frame data: " + frameId);
        }

        // Get layer data
        Map<Object, Object> layerData = redisTemplate.opsForHash().entries(LAYER_PREFIX + layerId);
        if (layerData == null || layerData.isEmpty()) {
            throw new RuntimeException("Layer data not found in Redis: " + layerId);
        }

        // Build DispatchFrame
        DispatchFrame frame = new DispatchFrame();

        // Frame fields
        frame.id = frameId;
        frame.layerId = layerId;
        frame.jobId = getString(frameData, "jobId");
        frame.name = getString(frameData, "name");
        frame.retries = getInt(frameData, "retries", 0);
        frame.state = FrameState.valueOf(getString(frameData, "state", "WAITING"));

        // Layer fields
        frame.layerName = getString(layerData, "name");
        frame.command = getString(layerData, "command");
        frame.range = getString(layerData, "range");
        frame.chunkSize = getInt(layerData, "chunkSize", 1);
        frame.services = getString(layerData, "services");
        frame.minCores = getInt(layerData, "minCores", 100);
        frame.maxCores = getInt(layerData, "maxCores", 0);
        frame.threadable = getBoolean(layerData, "threadable", false);
        frame.minGpus = getInt(layerData, "minGpus", 0);
        frame.maxGpus = getInt(layerData, "maxGpus", 0);
        frame.minGpuMemory = getLong(layerData, "minGpuMemory", 0);
        frame.setMinMemory(getLong(layerData, "minMemory", 0));

        // Job fields
        frame.show = getString(jobData, "showName");
        frame.shot = getString(jobData, "shot");
        frame.owner = getString(jobData, "owner");
        frame.uid = getOptionalInt(jobData, "uid");
        frame.logDir = getString(jobData, "logDir");
        frame.jobName = getString(jobData, "jobName");
        frame.os = getString(jobData, "os");
        frame.lokiURL = getString(jobData, "lokiURL");

        // Set IDs from job data if not in frame
        if (frame.showId == null) {
            frame.showId = getString(jobData, "showId");
        }
        if (frame.facilityId == null) {
            frame.facilityId = getString(jobData, "facilityId");
        }

        return frame;
    }

    // Helper methods for safe type conversion
    private String getString(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : "";
    }

    private String getString(Map<Object, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null && !value.toString().isEmpty() ? value.toString() : defaultValue;
    }

    private int getInt(Map<Object, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long getLong(Map<Object, Object> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(Map<Object, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private Optional<Integer> getOptionalInt(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        if (value == null || value.toString().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(value.toString()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Execute the Lua script to find eligible frame IDs.
     */
    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearch(JobInterface job, DispatchHost host, int limit) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();

        // Determine thread mode value
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;

        // Execute Lua script
        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(host.idleGpus),
                String.valueOf(host.idleGpuMemory),
                host.tags,
                String.valueOf(threadMode),
                String.valueOf(limit)
        );
    }

    /**
     * Execute frame search using VirtualProc resources.
     */
    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByProc(JobInterface job, VirtualProc proc, int limit) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();

        // For proc-based dispatch, use reserved resources
        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(proc.coresReserved),
                String.valueOf(proc.memoryReserved),
                String.valueOf(proc.gpusReserved),
                String.valueOf(proc.gpuMemoryReserved),
                proc.tags,
                "1", // Proc dispatch doesn't check threadable
                String.valueOf(limit)
        );
    }

    /**
     * Check if Redis has data for a job.
     * Useful for deciding whether to use Redis or fall back to SQL.
     */
    public boolean hasJobData(String jobId) {
        try {
            String key = LAYERS_WAITING_PREFIX + jobId;
            Long size = redisTemplate.opsForSet().size(key);
            return size != null && size > 0;
        } catch (Exception e) {
            logger.debug("Failed to check Redis for job {}: {}", jobId, e.getMessage());
            return false;
        }
    }
}


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
import com.imageworks.spcue.DispatchJob;
import com.imageworks.spcue.GroupInterface;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.ShowInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.FrameDao;
import com.imageworks.spcue.grpc.host.ThreadMode;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Redis-based dispatcher DAO for fast frame and job lookups.
 *
 * Builds DispatchFrame objects entirely from Redis hashes for ZERO SQL on hot path.
 * Falls back to SQL only if Redis data is missing (cache miss).
 *
 * Redis data structures used:
 * - frame:{frameId} - Frame metadata hash
 * - layer:{layerId} - Layer metadata hash
 * - job:{jobId} - Job metadata hash
 * - layers:waiting:{jobId} - Set of layer IDs with waiting frames
 * - frames:waiting:{layerId} - Sorted set of waiting frames
 * - jobs:pending:{showId}:{facilityId} - Set of pending jobs for show
 * - jobs:pending:group:{groupId} - Set of pending jobs for group
 */
@Repository
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatcherDao {

    private static final Logger logger = LogManager.getLogger(RedisDispatcherDao.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<List> findDispatchFramesScript;
    private final RedisScript<List> findDispatchFramesByLayerScript;
    private final RedisScript<List> findJobsByShowScript;
    private final FrameDao frameDao;

    // Redis key prefixes
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String JOB_PREFIX = "job:";
    private static final String JOBS_PENDING_PREFIX = "jobs:pending:";
    private static final String JOBS_PENDING_GROUP_PREFIX = "jobs:pending:group:";
    private static final String LAYER_LIMITS_PREFIX = "layer:limits:";

    // Sort modes for job queries
    public static final int SORT_MODE_BALANCED = 0;
    public static final int SORT_MODE_PRIORITY = 1;
    public static final int SORT_MODE_FIFO = 2;

    public RedisDispatcherDao(RedisTemplate<String, String> redisTemplate,
                               RedisScript<List> findDispatchFramesScript,
                               RedisScript<List> findDispatchFramesByLayerScript,
                               RedisScript<List> findJobsByShowScript,
                               FrameDao frameDao) {
        this.redisTemplate = redisTemplate;
        this.findDispatchFramesScript = findDispatchFramesScript;
        this.findDispatchFramesByLayerScript = findDispatchFramesByLayerScript;
        this.findJobsByShowScript = findJobsByShowScript;
        this.frameDao = frameDao;
        logger.info("Redis dispatcher DAO initialized (full Redis mode)");
    }

    // ============================================================
    // FRAME DISPATCH METHODS
    // ============================================================

    /**
     * Find next dispatch frames for a job using Redis.
     * Builds DispatchFrame entirely from Redis - zero SQL on hot path.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit) {
        return findNextDispatchFrames(job, host, limit, false);
    }

    /**
     * Find next dispatch frames for a job (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(JobInterface job, DispatchHost host, int limit) {
        return findNextDispatchFrames(job, host, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearch(job, host, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                logger.debug("No eligible frames found in Redis for job {}", job.getJobId());
                return Collections.emptyList();
            }

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
        return findNextDispatchFrames(job, proc, limit, false);
    }

    /**
     * Find next dispatch frames for a job using a VirtualProc (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(JobInterface job, VirtualProc proc, int limit) {
        return findNextDispatchFrames(job, proc, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByProc(job, proc, limit, noGpu);

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
     * Find next dispatch frames for a specific layer using Redis.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit) {
        return findNextDispatchFrames(layer, host, limit, false);
    }

    /**
     * Find next dispatch frames for a specific layer (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(LayerInterface layer, DispatchHost host, int limit) {
        return findNextDispatchFrames(layer, host, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByLayer(layer, host, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                logger.debug("No eligible frames found in Redis for layer {}", layer.getLayerId());
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, layer.getJobId());

            logger.debug("Redis findNextDispatchFrames (layer): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by layer failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find next dispatch frames for a specific layer using a VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit) {
        return findNextDispatchFrames(layer, proc, limit, false);
    }

    /**
     * Find next dispatch frames for a specific layer using a VirtualProc (NO_GPU variant).
     */
    public List<DispatchFrame> findNextDispatchFramesNoGpu(LayerInterface layer, VirtualProc proc, int limit) {
        return findNextDispatchFrames(layer, proc, limit, true);
    }

    private List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            List<String> frameIds = executeFrameSearchByLayerAndProc(layer, proc, limit, noGpu);

            if (frameIds == null || frameIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<DispatchFrame> frames = buildDispatchFramesFromRedis(frameIds, layer.getJobId());

            logger.debug("Redis findNextDispatchFrames (layer+proc): found {} frames in {}ms (zero SQL)",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by layer and proc failed", e);
            return Collections.emptyList();
        }
    }

    // ============================================================
    // JOB DISPATCH METHODS
    // ============================================================

    /**
     * Find jobs by show for dispatch (BALANCED mode - default).
     */
    public List<DispatchJob> findDispatchJobs(ShowInterface show, DispatchHost host, int limit) {
        return findDispatchJobs(show, host, limit, SORT_MODE_BALANCED, false);
    }

    /**
     * Find jobs by show for dispatch (BALANCED mode, NO_GPU).
     */
    public List<DispatchJob> findDispatchJobsNoGpu(ShowInterface show, DispatchHost host, int limit) {
        return findDispatchJobs(show, host, limit, SORT_MODE_BALANCED, true);
    }

    /**
     * Find jobs by show for dispatch (PRIORITY mode).
     */
    public List<DispatchJob> findDispatchJobsPriorityMode(ShowInterface show, DispatchHost host, int limit) {
        return findDispatchJobs(show, host, limit, SORT_MODE_PRIORITY, false);
    }

    /**
     * Find jobs by show for dispatch (FIFO mode).
     */
    public List<DispatchJob> findDispatchJobsFifoMode(ShowInterface show, DispatchHost host, int limit) {
        return findDispatchJobs(show, host, limit, SORT_MODE_FIFO, false);
    }

    private List<DispatchJob> findDispatchJobs(ShowInterface show, DispatchHost host, int limit, int sortMode, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            String jobsPendingKey = JOBS_PENDING_PREFIX + show.getShowId() + ":" + host.getFacilityId();
            List<List<Object>> results = executeJobSearch(jobsPendingKey, host, limit, sortMode, noGpu);

            if (results == null || results.isEmpty()) {
                logger.debug("No eligible jobs found in Redis for show {}", show.getShowId());
                return Collections.emptyList();
            }

            List<DispatchJob> jobs = buildDispatchJobsFromResults(results);

            logger.debug("Redis findDispatchJobs (show): found {} jobs in {}ms (zero SQL)",
                    jobs.size(), System.currentTimeMillis() - startTime);

            return jobs;

        } catch (Exception e) {
            logger.error("Redis job search failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * Find jobs by group for dispatch (BALANCED mode - default).
     */
    public List<DispatchJob> findDispatchJobs(GroupInterface group, DispatchHost host, int limit) {
        return findDispatchJobsByGroup(group, host, limit, SORT_MODE_BALANCED, false);
    }

    /**
     * Find jobs by group for dispatch (BALANCED mode, NO_GPU).
     */
    public List<DispatchJob> findDispatchJobsNoGpu(GroupInterface group, DispatchHost host, int limit) {
        return findDispatchJobsByGroup(group, host, limit, SORT_MODE_BALANCED, true);
    }

    /**
     * Find jobs by group for dispatch (PRIORITY mode).
     */
    public List<DispatchJob> findDispatchJobsPriorityMode(GroupInterface group, DispatchHost host, int limit) {
        return findDispatchJobsByGroup(group, host, limit, SORT_MODE_PRIORITY, false);
    }

    /**
     * Find jobs by group for dispatch (FIFO mode).
     */
    public List<DispatchJob> findDispatchJobsFifoMode(GroupInterface group, DispatchHost host, int limit) {
        return findDispatchJobsByGroup(group, host, limit, SORT_MODE_FIFO, false);
    }

    private List<DispatchJob> findDispatchJobsByGroup(GroupInterface group, DispatchHost host, int limit, int sortMode, boolean noGpu) {
        long startTime = System.currentTimeMillis();

        try {
            String jobsPendingKey = JOBS_PENDING_GROUP_PREFIX + group.getGroupId();
            List<List<Object>> results = executeJobSearch(jobsPendingKey, host, limit, sortMode, noGpu);

            if (results == null || results.isEmpty()) {
                logger.debug("No eligible jobs found in Redis for group {}", group.getGroupId());
                return Collections.emptyList();
            }

            List<DispatchJob> jobs = buildDispatchJobsFromResults(results);

            logger.debug("Redis findDispatchJobs (group): found {} jobs in {}ms (zero SQL)",
                    jobs.size(), System.currentTimeMillis() - startTime);

            return jobs;

        } catch (Exception e) {
            logger.error("Redis job search by group failed", e);
            return Collections.emptyList();
        }
    }

    // ============================================================
    // INTERNAL HELPER METHODS
    // ============================================================

    /**
     * Build DispatchFrame objects from Redis hashes.
     */
    private List<DispatchFrame> buildDispatchFramesFromRedis(List<String> frameIds, String jobId) {
        List<DispatchFrame> frames = new ArrayList<>(frameIds.size());

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
     */
    private DispatchFrame buildDispatchFrameFromRedis(String frameId, Map<Object, Object> jobData) {
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(FRAME_PREFIX + frameId);
        if (frameData == null || frameData.isEmpty()) {
            throw new RuntimeException("Frame data not found in Redis: " + frameId);
        }

        String layerId = getString(frameData, "layerId");
        if (layerId == null) {
            throw new RuntimeException("Layer ID not found in frame data: " + frameId);
        }

        Map<Object, Object> layerData = redisTemplate.opsForHash().entries(LAYER_PREFIX + layerId);
        if (layerData == null || layerData.isEmpty()) {
            throw new RuntimeException("Layer data not found in Redis: " + layerId);
        }

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

        if (frame.showId == null) {
            frame.showId = getString(jobData, "showId");
        }
        if (frame.facilityId == null) {
            frame.facilityId = getString(jobData, "facilityId");
        }

        return frame;
    }

    /**
     * Build DispatchJob objects from Lua script results.
     */
    private List<DispatchJob> buildDispatchJobsFromResults(List<List<Object>> results) {
        List<DispatchJob> jobs = new ArrayList<>(results.size());

        for (List<Object> row : results) {
            if (row != null && row.size() >= 3) {
                DispatchJob job = new DispatchJob();
                job.id = row.get(0).toString();
                job.priority = ((Number) row.get(1)).intValue();
                job.rank = ((Number) row.get(2)).intValue();
                jobs.add(job);
            }
        }

        return jobs;
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

    // ============================================================
    // LUA SCRIPT EXECUTION METHODS
    // ============================================================

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearch(JobInterface job, DispatchHost host, int limit, boolean noGpu) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;

        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(host.idleGpus),
                String.valueOf(host.idleGpuMemory),
                host.tags,
                String.valueOf(threadMode),
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByProc(JobInterface job, VirtualProc proc, int limit, boolean noGpu) {
        String layersWaitingKey = LAYERS_WAITING_PREFIX + job.getJobId();

        return redisTemplate.execute(
                findDispatchFramesScript,
                Collections.singletonList(layersWaitingKey),
                String.valueOf(proc.coresReserved),
                String.valueOf(proc.memoryReserved),
                String.valueOf(proc.gpusReserved),
                String.valueOf(proc.gpuMemoryReserved),
                proc.tags,
                "1", // Proc dispatch doesn't check threadable
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByLayer(LayerInterface layer, DispatchHost host, int limit, boolean noGpu) {
        String layerKey = LAYER_PREFIX + layer.getLayerId();
        String framesWaitingKey = FRAMES_WAITING_PREFIX + layer.getLayerId();
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layer.getLayerId();
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;

        return redisTemplate.execute(
                findDispatchFramesByLayerScript,
                java.util.Arrays.asList(layerKey, framesWaitingKey, layerLimitsKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(host.idleGpus),
                String.valueOf(host.idleGpuMemory),
                host.tags,
                String.valueOf(threadMode),
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> executeFrameSearchByLayerAndProc(LayerInterface layer, VirtualProc proc, int limit, boolean noGpu) {
        String layerKey = LAYER_PREFIX + layer.getLayerId();
        String framesWaitingKey = FRAMES_WAITING_PREFIX + layer.getLayerId();
        String layerLimitsKey = LAYER_LIMITS_PREFIX + layer.getLayerId();

        return redisTemplate.execute(
                findDispatchFramesByLayerScript,
                java.util.Arrays.asList(layerKey, framesWaitingKey, layerLimitsKey),
                String.valueOf(proc.coresReserved),
                String.valueOf(proc.memoryReserved),
                String.valueOf(proc.gpusReserved),
                String.valueOf(proc.gpuMemoryReserved),
                proc.tags,
                "1", // Proc dispatch doesn't check threadable
                String.valueOf(limit),
                noGpu ? "1" : "0"
        );
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> executeJobSearch(String jobsPendingKey, DispatchHost host, int limit, int sortMode, boolean noGpu) {
        int threadMode = (host.threadMode == ThreadMode.ALL_VALUE) ? 1 : 0;
        long currentTime = System.currentTimeMillis() / 1000;

        return redisTemplate.execute(
                findJobsByShowScript,
                Collections.singletonList(jobsPendingKey),
                String.valueOf(host.idleCores),
                String.valueOf(host.idleMemory),
                String.valueOf(threadMode),
                String.valueOf(host.idleGpus),
                String.valueOf(0), // minGpuMem
                String.valueOf(host.idleGpuMemory), // maxGpuMem
                host.tags,
                host.os,
                String.valueOf(currentTime),
                String.valueOf(limit),
                String.valueOf(sortMode),
                noGpu ? "1" : "0"
        );
    }

    /**
     * Check if Redis has data for a job.
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

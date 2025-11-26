
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

/**
 * Redis-based dispatcher DAO for fast frame lookups.
 *
 * This is a supplementary DAO that uses Redis for the expensive filtering
 * operations (finding eligible frames), then delegates to the SQL DAO
 * for fetching full frame details.
 *
 * Usage pattern:
 * 1. Use Lua script to find eligible frame IDs in Redis (O(n) with small n)
 * 2. Fetch full DispatchFrame objects from SQL using the IDs
 *
 * This hybrid approach gives us:
 * - Speed: Redis filtering is much faster than SQL JOINs
 * - Consistency: SQL remains source of truth for frame details
 * - Simplicity: No need to sync all frame details to Redis
 */
@Repository
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatcherDao {

    private static final Logger logger = LogManager.getLogger(RedisDispatcherDao.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<List> findDispatchFramesScript;
    private final FrameDao frameDao;

    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";

    public RedisDispatcherDao(RedisTemplate<String, String> redisTemplate,
                               RedisScript<List> findDispatchFramesScript,
                               FrameDao frameDao) {
        this.redisTemplate = redisTemplate;
        this.findDispatchFramesScript = findDispatchFramesScript;
        this.frameDao = frameDao;
        logger.info("Redis dispatcher DAO initialized");
    }

    /**
     * Find next dispatch frames for a job using Redis.
     *
     * This mirrors DispatcherDaoJdbc.findNextDispatchFrames(JobInterface, DispatchHost, int)
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

            // Fetch full DispatchFrame objects from SQL
            List<DispatchFrame> frames = new ArrayList<>(frameIds.size());
            for (String frameId : frameIds) {
                try {
                    DispatchFrame frame = frameDao.getDispatchFrame(frameId);
                    if (frame != null) {
                        frames.add(frame);
                    }
                } catch (Exception e) {
                    // Frame may have been dispatched between Redis lookup and SQL fetch
                    logger.debug("Frame {} no longer available: {}", frameId, e.getMessage());
                }
            }

            logger.debug("Redis findNextDispatchFrames: found {} frames in {}ms",
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

            List<DispatchFrame> frames = new ArrayList<>(frameIds.size());
            for (String frameId : frameIds) {
                try {
                    DispatchFrame frame = frameDao.getDispatchFrame(frameId);
                    if (frame != null) {
                        frames.add(frame);
                    }
                } catch (Exception e) {
                    logger.debug("Frame {} no longer available: {}", frameId, e.getMessage());
                }
            }

            logger.debug("Redis findNextDispatchFrames (proc): found {} frames in {}ms",
                    frames.size(), System.currentTimeMillis() - startTime);

            return frames;

        } catch (Exception e) {
            logger.error("Redis frame search by proc failed", e);
            return Collections.emptyList();
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

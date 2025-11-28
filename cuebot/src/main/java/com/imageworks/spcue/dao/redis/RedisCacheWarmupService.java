
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Populates Redis cache from SQL on application startup.
 *
 * This ensures Redis has correct data after a cuebot restart.
 * Once populated, incremental updates are handled by event publishing.
 *
 * The warm-up runs SYNCHRONOUSLY at startup - the application will not
 * accept scheduling requests until warmup is complete. This is simpler
 * and safer than async warmup with ready flags.
 */
@Service
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisCacheWarmupService {

    private static final Logger logger = LogManager.getLogger(RedisCacheWarmupService.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final JdbcTemplate jdbcTemplate;

    // Redis key prefixes (same as in RedisSchedulingEventListener)
    // Note: No JOB_PREFIX - job data is cached in-memory by RedisDispatcherDao
    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String FRAME_PREFIX = "frame:";
    private static final String LAYER_PREFIX = "layer:";
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String LIMIT_PREFIX = "limit:";
    private static final String LAYER_LIMITS_PREFIX = "layer:limits:";

    @Autowired
    public RedisCacheWarmupService(RedisTemplate<String, String> redisTemplate,
                                    JdbcTemplate jdbcTemplate) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Warm up Redis cache at application startup.
     * Runs SYNCHRONOUSLY - application won't accept requests until complete.
     */
    @PostConstruct
    public void init() {
        logger.info("Starting synchronous Redis cache warm-up...");
        warmupCache();
    }

    /**
     * Main warm-up method - populates Redis from SQL.
     * Note: Job metadata is cached in-memory by RedisDispatcherDao, not in Redis.
     */
    public void warmupCache() {
        long startTime = System.currentTimeMillis();
        logger.info("Starting Redis cache warm-up from SQL...");

        try {
            // Clear existing scheduling data (in case of stale data)
            clearSchedulingData();

            // Populate limits first (they're referenced by layers)
            int limitCount = warmupLimits();

            // Populate layers and their limits
            int layerCount = warmupLayers();

            // Populate waiting frames
            int frameCount = warmupWaitingFrames();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Redis cache warm-up completed in {}ms: {} layers, {} frames, {} limits",
                    duration, layerCount, frameCount, limitCount);

        } catch (Exception e) {
            logger.error("Redis cache warm-up failed", e);
            // Don't throw - the system can still work with SQL fallback
        }
    }

    /**
     * Clear existing scheduling data from Redis.
     * Uses SCAN to find and delete keys without blocking Redis for too long.
     */
    private void clearSchedulingData() {
        logger.info("Clearing existing scheduling data from Redis...");

        // Note: job:* not included - job data is cached in-memory, not Redis
        String[] patterns = {
            "frame:*",
            "layer:*",
            "frames:waiting:*",
            "layers:waiting:*",
            "limit:*"
        };

        int totalDeleted = 0;
        for (String pattern : patterns) {
            Set<String> keysToDelete = new HashSet<>();

            // Use SCAN via keys() - Spring Data Redis handles cursor internally
            // Note: For very large datasets, consider using scan() with ScanOptions
            var keys = redisTemplate.keys(pattern);
            if (keys != null) {
                keysToDelete.addAll(keys);
            }

            if (!keysToDelete.isEmpty()) {
                redisTemplate.delete(keysToDelete);
                totalDeleted += keysToDelete.size();
                logger.debug("Deleted {} keys matching pattern {}", keysToDelete.size(), pattern);
            }
        }

        logger.info("Cleared {} existing scheduling keys from Redis", totalDeleted);
    }

    /**
     * Warm up limit data.
     */
    private int warmupLimits() {
        String sql = "SELECT lr.pk_limit_record, lr.str_name, lr.int_max_value, " +
                     "COALESCE(SUM(ls.int_running_count), 0) AS int_running " +
                     "FROM limit_record lr " +
                     "LEFT JOIN layer_limit ll ON ll.pk_limit_record = lr.pk_limit_record " +
                     "LEFT JOIN layer_stat ls ON ls.pk_layer = ll.pk_layer " +
                     "GROUP BY lr.pk_limit_record, lr.str_name, lr.int_max_value";

        List<Map<String, Object>> limits = jdbcTemplate.queryForList(sql);
        int count = 0;

        for (Map<String, Object> row : limits) {
            String limitId = (String) row.get("pk_limit_record");
            int maxValue = ((Number) row.get("int_max_value")).intValue();
            int running = ((Number) row.get("int_running")).intValue();

            // Store limit metadata
            String limitKey = LIMIT_PREFIX + limitId;
            redisTemplate.opsForHash().put(limitKey, "maxValue", String.valueOf(maxValue));
            redisTemplate.opsForHash().put(limitKey, "name", (String) row.get("str_name"));

            // Store running count
            String runningKey = LIMIT_PREFIX + limitId + ":running";
            redisTemplate.opsForValue().set(runningKey, String.valueOf(running));

            count++;
        }

        logger.debug("Warmed up {} limits", count);
        return count;
    }

    /**
     * Warm up layers for pending jobs.
     */
    private int warmupLayers() {
        String sql = "SELECT l.pk_layer, l.pk_job, l.str_name, l.str_type, l.str_tags, " +
                     "l.str_cmd, l.str_range, l.int_chunk_size, l.str_services, " +
                     "l.int_cores_min, l.int_cores_max, l.int_mem_min, " +
                     "l.int_gpus_min, l.int_gpus_max, l.int_gpu_mem_min, l.b_threadable, " +
                     "ls.int_waiting_count " +
                     "FROM layer l " +
                     "JOIN job j ON j.pk_job = l.pk_job " +
                     "JOIN layer_stat ls ON ls.pk_layer = l.pk_layer " +
                     "WHERE j.str_state = 'PENDING' AND j.b_paused = false";

        List<Map<String, Object>> layers = jdbcTemplate.queryForList(sql);
        int count = 0;

        for (Map<String, Object> row : layers) {
            String layerId = (String) row.get("pk_layer");
            String jobId = (String) row.get("pk_job");
            int waitingCount = ((Number) row.get("int_waiting_count")).intValue();

            // Store layer metadata
            String layerKey = LAYER_PREFIX + layerId;
            Map<String, String> layerData = new HashMap<>();
            layerData.put("jobId", jobId);
            layerData.put("name", (String) row.get("str_name"));
            layerData.put("type", (String) row.get("str_type"));
            layerData.put("tags", nullToEmpty(row.get("str_tags")));
            layerData.put("command", nullToEmpty(row.get("str_cmd")));
            layerData.put("range", nullToEmpty(row.get("str_range")));
            layerData.put("chunkSize", String.valueOf(row.get("int_chunk_size")));
            layerData.put("services", nullToEmpty(row.get("str_services")));
            layerData.put("minCores", String.valueOf(row.get("int_cores_min")));
            layerData.put("maxCores", String.valueOf(row.get("int_cores_max")));
            layerData.put("minMemory", String.valueOf(row.get("int_mem_min")));
            layerData.put("minGpus", String.valueOf(row.get("int_gpus_min")));
            layerData.put("maxGpus", String.valueOf(row.get("int_gpus_max")));
            layerData.put("minGpuMemory", String.valueOf(row.get("int_gpu_mem_min")));
            layerData.put("threadable", String.valueOf(row.get("b_threadable")));

            redisTemplate.opsForHash().putAll(layerKey, layerData);

            // Track layers with waiting frames
            if (waitingCount > 0) {
                redisTemplate.opsForSet().add(LAYERS_WAITING_PREFIX + jobId, layerId);
            }

            // Store layer limits
            warmupLayerLimits(layerId);

            count++;
        }

        logger.debug("Warmed up {} layers", count);
        return count;
    }

    /**
     * Warm up limits for a specific layer.
     */
    private void warmupLayerLimits(String layerId) {
        String sql = "SELECT ll.pk_limit_record, lr.int_max_value " +
                     "FROM layer_limit ll " +
                     "JOIN limit_record lr ON lr.pk_limit_record = ll.pk_limit_record " +
                     "WHERE ll.pk_layer = ?";

        List<Map<String, Object>> limits = jdbcTemplate.queryForList(sql, layerId);

        if (!limits.isEmpty()) {
            String layerLimitsKey = LAYER_LIMITS_PREFIX + layerId;
            for (Map<String, Object> row : limits) {
                String limitId = (String) row.get("pk_limit_record");
                redisTemplate.opsForSet().add(layerLimitsKey, limitId);
            }
        }
    }

    /**
     * Warm up waiting frames.
     */
    private int warmupWaitingFrames() {
        String sql = "SELECT f.pk_frame, f.pk_layer, f.pk_job, f.str_name, " +
                     "f.int_dispatch_order, f.int_layer_order, f.int_retries, f.int_version " +
                     "FROM frame f " +
                     "JOIN job j ON j.pk_job = f.pk_job " +
                     "WHERE f.str_state = 'WAITING' " +
                     "AND j.str_state = 'PENDING' AND j.b_paused = false";

        List<Map<String, Object>> frames = jdbcTemplate.queryForList(sql);
        int count = 0;

        for (Map<String, Object> row : frames) {
            String frameId = (String) row.get("pk_frame");
            String layerId = (String) row.get("pk_layer");
            String jobId = (String) row.get("pk_job");
            int dispatchOrder = ((Number) row.get("int_dispatch_order")).intValue();
            int layerOrder = ((Number) row.get("int_layer_order")).intValue();

            // Calculate sort score (same as event listener and SQL ORDER BY)
            // SQL: ORDER BY frame.int_dispatch_order ASC, frame.int_layer_order ASC
            // dispatchOrder is primary, layerOrder is secondary (tiebreaker)
            double sortScore = dispatchOrder + (layerOrder / 1000000.0);

            // Add to waiting frames sorted set
            String waitingKey = FRAMES_WAITING_PREFIX + layerId;
            redisTemplate.opsForZSet().add(waitingKey, frameId, sortScore);

            // Store frame metadata
            String frameKey = FRAME_PREFIX + frameId;
            Map<String, String> frameData = new HashMap<>();
            frameData.put("layerId", layerId);
            frameData.put("jobId", jobId);
            frameData.put("state", "WAITING");
            frameData.put("dispatchOrder", String.valueOf(dispatchOrder));
            frameData.put("layerOrder", String.valueOf(layerOrder));
            frameData.put("name", nullToEmpty(row.get("str_name")));
            frameData.put("retries", String.valueOf(row.get("int_retries")));
            frameData.put("version", String.valueOf(row.get("int_version")));

            redisTemplate.opsForHash().putAll(frameKey, frameData);
            count++;
        }

        logger.debug("Warmed up {} waiting frames", count);
        return count;
    }

    private String nullToEmpty(Object value) {
        return value != null ? value.toString() : "";
    }

    // ============================================================
    // SINGLE JOB WARMUP (called when new job is launched)
    // ============================================================

    /**
     * Warm up Redis cache for a single job.
     * Called when a new job is launched AFTER the initial startup warmup.
     *
     * This populates layers and frames for the job so Redis queries work
     * immediately without falling back to SQL.
     *
     * @param jobId The job ID to warm up
     */
    public void warmupJob(String jobId) {
        long startTime = System.currentTimeMillis();
        logger.info("Warming up Redis cache for job: {}", jobId);

        try {
            // Note: Job metadata is cached in-memory by RedisDispatcherDao, not Redis
            int layerCount = warmupJobLayers(jobId);
            int frameCount = warmupJobFrames(jobId);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Job {} warmed up in {}ms: {} layers, {} frames",
                    jobId, duration, layerCount, frameCount);

        } catch (Exception e) {
            logger.error("Failed to warm up job {} in Redis", jobId, e);
            // Don't throw - SQL fallback will work
        }
    }

    /**
     * Warm up layers for a specific job.
     */
    private int warmupJobLayers(String jobId) {
        String sql = "SELECT l.pk_layer, l.pk_job, l.str_name, l.str_type, l.str_tags, " +
                     "l.str_cmd, l.str_range, l.int_chunk_size, l.str_services, " +
                     "l.int_cores_min, l.int_cores_max, l.int_mem_min, " +
                     "l.int_gpus_min, l.int_gpus_max, l.int_gpu_mem_min, l.b_threadable, " +
                     "ls.int_waiting_count " +
                     "FROM layer l " +
                     "JOIN layer_stat ls ON ls.pk_layer = l.pk_layer " +
                     "WHERE l.pk_job = ?";

        List<Map<String, Object>> layers = jdbcTemplate.queryForList(sql, jobId);
        int count = 0;

        for (Map<String, Object> row : layers) {
            String layerId = (String) row.get("pk_layer");
            int waitingCount = ((Number) row.get("int_waiting_count")).intValue();

            // Store layer metadata
            String layerKey = LAYER_PREFIX + layerId;
            Map<String, String> layerData = new HashMap<>();
            layerData.put("jobId", jobId);
            layerData.put("name", (String) row.get("str_name"));
            layerData.put("type", (String) row.get("str_type"));
            layerData.put("tags", nullToEmpty(row.get("str_tags")));
            layerData.put("command", nullToEmpty(row.get("str_cmd")));
            layerData.put("range", nullToEmpty(row.get("str_range")));
            layerData.put("chunkSize", String.valueOf(row.get("int_chunk_size")));
            layerData.put("services", nullToEmpty(row.get("str_services")));
            layerData.put("minCores", String.valueOf(row.get("int_cores_min")));
            layerData.put("maxCores", String.valueOf(row.get("int_cores_max")));
            layerData.put("minMemory", String.valueOf(row.get("int_mem_min")));
            layerData.put("minGpus", String.valueOf(row.get("int_gpus_min")));
            layerData.put("maxGpus", String.valueOf(row.get("int_gpus_max")));
            layerData.put("minGpuMemory", String.valueOf(row.get("int_gpu_mem_min")));
            layerData.put("threadable", String.valueOf(row.get("b_threadable")));

            redisTemplate.opsForHash().putAll(layerKey, layerData);

            // Track layers with waiting frames
            if (waitingCount > 0) {
                redisTemplate.opsForSet().add(LAYERS_WAITING_PREFIX + jobId, layerId);
            }

            // Store layer limits
            warmupLayerLimits(layerId);

            count++;
        }

        logger.debug("Warmed up {} layers for job {}", count, jobId);
        return count;
    }

    /**
     * Warm up waiting frames for a specific job.
     */
    private int warmupJobFrames(String jobId) {
        String sql = "SELECT f.pk_frame, f.pk_layer, f.pk_job, f.str_name, " +
                     "f.int_dispatch_order, f.int_layer_order, f.int_retries, f.int_version " +
                     "FROM frame f " +
                     "WHERE f.pk_job = ? AND f.str_state = 'WAITING'";

        List<Map<String, Object>> frames = jdbcTemplate.queryForList(sql, jobId);
        int count = 0;

        for (Map<String, Object> row : frames) {
            String frameId = (String) row.get("pk_frame");
            String layerId = (String) row.get("pk_layer");
            int dispatchOrder = ((Number) row.get("int_dispatch_order")).intValue();
            int layerOrder = ((Number) row.get("int_layer_order")).intValue();

            // Calculate sort score (same as event listener and SQL ORDER BY)
            double sortScore = dispatchOrder + (layerOrder / 1000000.0);

            // Add to waiting frames sorted set
            String waitingKey = FRAMES_WAITING_PREFIX + layerId;
            redisTemplate.opsForZSet().add(waitingKey, frameId, sortScore);

            // Store frame metadata
            String frameKey = FRAME_PREFIX + frameId;
            Map<String, String> frameData = new HashMap<>();
            frameData.put("layerId", layerId);
            frameData.put("jobId", jobId);
            frameData.put("state", "WAITING");
            frameData.put("dispatchOrder", String.valueOf(dispatchOrder));
            frameData.put("layerOrder", String.valueOf(layerOrder));
            frameData.put("name", nullToEmpty(row.get("str_name")));
            frameData.put("retries", String.valueOf(row.get("int_retries")));
            frameData.put("version", String.valueOf(row.get("int_version")));

            redisTemplate.opsForHash().putAll(frameKey, frameData);
            count++;
        }

        logger.debug("Warmed up {} waiting frames for job {}", count, jobId);
        return count;
    }
}

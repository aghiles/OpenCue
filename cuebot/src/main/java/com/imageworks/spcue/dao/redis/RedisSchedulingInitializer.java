
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

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Initializes Redis scheduling cache from existing SQL data.
 *
 * On startup (when Redis is enabled), this component:
 * 1. Scans pending jobs in SQL
 * 2. Loads waiting frames and layer metadata into Redis
 * 3. Enables Redis-based dispatch for those jobs
 *
 * This ensures Redis has data even for jobs that were created
 * before Redis was enabled.
 */
@Component
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisSchedulingInitializer {

    private static final Logger logger = LogManager.getLogger(RedisSchedulingInitializer.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final JdbcTemplate jdbcTemplate;

    @Value("${redis.scheduling.init.enabled:true}")
    private boolean initEnabled;

    @Value("${redis.scheduling.init.batch_size:1000}")
    private int batchSize;

    private static final String FRAMES_WAITING_PREFIX = "frames:waiting:";
    private static final String LAYERS_WAITING_PREFIX = "layers:waiting:";
    private static final String LAYER_PREFIX = "layer:";

    public RedisSchedulingInitializer(RedisTemplate<String, String> redisTemplate,
                                       JdbcTemplate jdbcTemplate) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    @Async
    public void initialize() {
        if (!initEnabled) {
            logger.info("Redis scheduling initialization disabled");
            return;
        }

        logger.info("Starting Redis scheduling cache initialization...");

        try {
            long startTime = System.currentTimeMillis();

            // Load pending jobs
            List<Map<String, Object>> pendingJobs = jdbcTemplate.queryForList(
                    "SELECT pk_job FROM job WHERE str_state = 'PENDING'"
            );

            logger.info("Found {} pending jobs to initialize", pendingJobs.size());

            int jobCount = 0;
            int frameCount = 0;
            int layerCount = 0;

            for (Map<String, Object> job : pendingJobs) {
                String jobId = (String) job.get("pk_job");

                // Load layers for this job
                int layers = initializeLayers(jobId);
                layerCount += layers;

                // Load waiting frames for each layer
                int frames = initializeWaitingFrames(jobId);
                frameCount += frames;

                jobCount++;

                if (jobCount % 100 == 0) {
                    logger.info("Initialized {} jobs, {} layers, {} frames...",
                            jobCount, layerCount, frameCount);
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Redis initialization complete: {} jobs, {} layers, {} frames in {}ms",
                    jobCount, layerCount, frameCount, duration);

        } catch (Exception e) {
            logger.error("Redis initialization failed - dispatch will use SQL", e);
        }
    }

    /**
     * Initialize layer metadata in Redis.
     */
    private int initializeLayers(String jobId) {
        List<Map<String, Object>> layers = jdbcTemplate.queryForList(
                "SELECT pk_layer, str_name, str_type, int_cores_min, int_cores_max, " +
                "int_mem_min, int_gpus_min, int_gpus_max, int_gpu_mem_min, " +
                "b_threadable, str_tags, str_cmd, str_range, int_chunk_size, str_services " +
                "FROM layer WHERE pk_job = ?",
                jobId
        );

        for (Map<String, Object> layer : layers) {
            String layerId = (String) layer.get("pk_layer");
            String layerKey = LAYER_PREFIX + layerId;

            try {
                redisTemplate.opsForHash().put(layerKey, "jobId", jobId);
                redisTemplate.opsForHash().put(layerKey, "name", nullSafe(layer.get("str_name")));
                redisTemplate.opsForHash().put(layerKey, "type", nullSafe(layer.get("str_type")));
                redisTemplate.opsForHash().put(layerKey, "minCores", String.valueOf(layer.get("int_cores_min")));
                redisTemplate.opsForHash().put(layerKey, "maxCores", String.valueOf(layer.get("int_cores_max")));
                redisTemplate.opsForHash().put(layerKey, "minMemory", String.valueOf(layer.get("int_mem_min")));
                redisTemplate.opsForHash().put(layerKey, "minGpus", String.valueOf(layer.get("int_gpus_min")));
                redisTemplate.opsForHash().put(layerKey, "maxGpus", String.valueOf(layer.get("int_gpus_max")));
                redisTemplate.opsForHash().put(layerKey, "minGpuMemory", String.valueOf(layer.get("int_gpu_mem_min")));
                redisTemplate.opsForHash().put(layerKey, "threadable", String.valueOf(layer.get("b_threadable")));
                redisTemplate.opsForHash().put(layerKey, "tags", nullSafe(layer.get("str_tags")));
                redisTemplate.opsForHash().put(layerKey, "command", nullSafe(layer.get("str_cmd")));
                redisTemplate.opsForHash().put(layerKey, "range", nullSafe(layer.get("str_range")));
                redisTemplate.opsForHash().put(layerKey, "chunkSize", String.valueOf(layer.get("int_chunk_size")));
                redisTemplate.opsForHash().put(layerKey, "services", nullSafe(layer.get("str_services")));
            } catch (Exception e) {
                logger.warn("Failed to initialize layer {}: {}", layerId, e.getMessage());
            }
        }

        return layers.size();
    }

    /**
     * Initialize waiting frames in Redis.
     */
    private int initializeWaitingFrames(String jobId) {
        List<Map<String, Object>> frames = jdbcTemplate.queryForList(
                "SELECT pk_frame, pk_layer, int_dispatch_order, int_layer_order " +
                "FROM frame WHERE pk_job = ? AND str_state = 'WAITING'",
                jobId
        );

        String layersWaitingKey = LAYERS_WAITING_PREFIX + jobId;

        for (Map<String, Object> frame : frames) {
            String frameId = (String) frame.get("pk_frame");
            String layerId = (String) frame.get("pk_layer");
            int dispatchOrder = ((Number) frame.get("int_dispatch_order")).intValue();
            int layerOrder = ((Number) frame.get("int_layer_order")).intValue();

            // Calculate sort score
            double score = dispatchOrder + (layerOrder / 1000000.0);

            try {
                // Add frame to waiting set
                String framesWaitingKey = FRAMES_WAITING_PREFIX + layerId;
                redisTemplate.opsForZSet().add(framesWaitingKey, frameId, score);

                // Track that this layer has waiting frames
                redisTemplate.opsForSet().add(layersWaitingKey, layerId);
            } catch (Exception e) {
                logger.warn("Failed to initialize frame {}: {}", frameId, e.getMessage());
            }
        }

        return frames.size();
    }

    private String nullSafe(Object value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Refresh Redis cache for a specific job.
     * Can be called manually or scheduled.
     */
    public void refreshJob(String jobId) {
        logger.info("Refreshing Redis cache for job {}", jobId);

        try {
            // Clear existing data
            String layersWaitingKey = LAYERS_WAITING_PREFIX + jobId;
            var layerIds = redisTemplate.opsForSet().members(layersWaitingKey);
            if (layerIds != null) {
                for (String layerId : layerIds) {
                    redisTemplate.delete(FRAMES_WAITING_PREFIX + layerId);
                }
            }
            redisTemplate.delete(layersWaitingKey);

            // Reinitialize
            initializeLayers(jobId);
            initializeWaitingFrames(jobId);

            logger.info("Redis cache refreshed for job {}", jobId);

        } catch (Exception e) {
            logger.error("Failed to refresh Redis cache for job {}", jobId, e);
        }
    }
}

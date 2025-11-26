
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.DispatchJob;
import com.imageworks.spcue.GroupInterface;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.ShowInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.dao.DispatcherDao;

/**
 * Redis-enhanced dispatch support.
 *
 * Provides a Redis-first approach for finding dispatch frames:
 * 1. Try Redis for fast frame lookup
 * 2. Fall back to SQL DAO if Redis fails or returns empty results
 *
 * This service is only active when redis.scheduling.enabled=true.
 */
@Service
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisDispatchSupport {

    private static final Logger logger = LogManager.getLogger(RedisDispatchSupport.class);

    private final RedisDispatcherDao redisDispatcherDao;
    private final DispatcherDao sqlDispatcherDao;

    @Autowired
    public RedisDispatchSupport(RedisDispatcherDao redisDispatcherDao,
                                 DispatcherDao sqlDispatcherDao) {
        this.redisDispatcherDao = redisDispatcherDao;
        this.sqlDispatcherDao = sqlDispatcherDao;
        logger.info("Redis dispatch support initialized - Redis-first dispatch enabled");
    }

    /**
     * Find next dispatch frames using Redis with SQL fallback.
     *
     * @param job   The job to find frames for
     * @param host  The host with available resources
     * @param limit Maximum frames to return
     * @return List of dispatchable frames
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, DispatchHost host, int limit) {
        long startTime = System.currentTimeMillis();

        // Check if Redis has data for this job
        if (redisDispatcherDao.hasJobData(job.getJobId())) {
            List<DispatchFrame> frames = redisDispatcherDao.findNextDispatchFrames(job, host, limit);

            if (!frames.isEmpty()) {
                logger.debug("Redis dispatch: found {} frames for job {} in {}ms",
                        frames.size(), job.getJobId(), System.currentTimeMillis() - startTime);
                return frames;
            }

            // Redis returned empty - could be a cache miss or truly no frames
            // Fall through to SQL to be safe
            logger.debug("Redis returned empty for job {}, falling back to SQL", job.getJobId());
        }

        // Fall back to SQL
        List<DispatchFrame> frames = sqlDispatcherDao.findNextDispatchFrames(job, host, limit);

        logger.debug("SQL dispatch fallback: found {} frames for job {} in {}ms",
                frames.size(), job.getJobId(), System.currentTimeMillis() - startTime);

        return frames;
    }

    /**
     * Find next dispatch frames using VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit) {
        long startTime = System.currentTimeMillis();

        if (redisDispatcherDao.hasJobData(job.getJobId())) {
            List<DispatchFrame> frames = redisDispatcherDao.findNextDispatchFrames(job, proc, limit);

            if (!frames.isEmpty()) {
                logger.debug("Redis dispatch (proc): found {} frames in {}ms",
                        frames.size(), System.currentTimeMillis() - startTime);
                return frames;
            }
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(job, proc, limit);
    }

    /**
     * Check if Redis dispatch is available for a job.
     */
    public boolean isRedisAvailableForJob(String jobId) {
        return redisDispatcherDao.hasJobData(jobId);
    }

    // ============================================================
    // LAYER DISPATCH METHODS
    // ============================================================

    /**
     * Find next dispatch frames for a specific layer using Redis with SQL fallback.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, DispatchHost host, int limit) {
        long startTime = System.currentTimeMillis();

        List<DispatchFrame> frames = redisDispatcherDao.findNextDispatchFrames(layer, host, limit);

        if (!frames.isEmpty()) {
            logger.debug("Redis dispatch (layer): found {} frames in {}ms",
                    frames.size(), System.currentTimeMillis() - startTime);
            return frames;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(layer, host, limit);
    }

    /**
     * Find next dispatch frames for a specific layer using VirtualProc.
     */
    public List<DispatchFrame> findNextDispatchFrames(LayerInterface layer, VirtualProc proc, int limit) {
        long startTime = System.currentTimeMillis();

        List<DispatchFrame> frames = redisDispatcherDao.findNextDispatchFrames(layer, proc, limit);

        if (!frames.isEmpty()) {
            logger.debug("Redis dispatch (layer+proc): found {} frames in {}ms",
                    frames.size(), System.currentTimeMillis() - startTime);
            return frames;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findNextDispatchFrames(layer, proc, limit);
    }

    // ============================================================
    // JOB DISPATCH METHODS
    // ============================================================

    /**
     * Find dispatch jobs for a show using Redis with SQL fallback.
     * Returns job IDs for compatibility with existing code.
     */
    public Set<String> findDispatchJobs(DispatchHost host, ShowInterface show, int numJobs) {
        long startTime = System.currentTimeMillis();

        List<DispatchJob> jobs = redisDispatcherDao.findDispatchJobs(show, host, numJobs);

        if (!jobs.isEmpty()) {
            Set<String> jobIds = new HashSet<>(jobs.size());
            for (DispatchJob job : jobs) {
                jobIds.add(job.id);
            }
            logger.debug("Redis dispatch (show): found {} jobs in {}ms",
                    jobs.size(), System.currentTimeMillis() - startTime);
            return jobIds;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findDispatchJobs(host, show, numJobs);
    }

    /**
     * Find dispatch jobs for a group using Redis with SQL fallback.
     */
    public Set<String> findDispatchJobs(DispatchHost host, GroupInterface group) {
        long startTime = System.currentTimeMillis();

        List<DispatchJob> jobs = redisDispatcherDao.findDispatchJobs(group, host, 50);

        if (!jobs.isEmpty()) {
            Set<String> jobIds = new HashSet<>(jobs.size());
            for (DispatchJob job : jobs) {
                jobIds.add(job.id);
            }
            logger.debug("Redis dispatch (group): found {} jobs in {}ms",
                    jobs.size(), System.currentTimeMillis() - startTime);
            return jobIds;
        }

        // Fall back to SQL
        return sqlDispatcherDao.findDispatchJobs(host, group);
    }
}

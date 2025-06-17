package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.JobDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.job.JobState;
import com.imageworks.spcue.grpc.job.Job;
import com.imageworks.spcue.util.CueUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Profile("redis")
public class JobDaoRedis implements JobDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    @Override
    public JobDetail getJobDetail(String id) {
        String key = RedisKeyBuilder.job(id);
        Map<Object, Object> jobData = redisTemplate.opsForHash().entries(key);
        
        if (jobData.isEmpty()) {
            throw new EntityNotFoundException("Job not found: " + id);
        }
        
        return redisDataMapper.mapToJobDetail(jobData);
    }
    
    @Override
    public JobInterface getJob(String id) {
        return getJobDetail(id);
    }
    
    @Override
    public JobInterface findJob(String name) {
        String indexKey = RedisKeyBuilder.jobNameIndex(name);
        String jobId = redisTemplate.opsForValue().get(indexKey);
        
        if (jobId == null) {
            return null;
        }
        
        return getJob(jobId);
    }
    
    @Override
    public void insertJob(JobDetail job) {
        String jobId = job.getJobId();
        String jobKey = RedisKeyBuilder.job(jobId);
        
        Map<String, String> jobData = new HashMap<>();
        jobData.put("id", jobId);
        jobData.put("name", job.getName());
        jobData.put("show_id", job.getShowId());
        jobData.put("facility_id", job.getFacilityId());
        jobData.put("dept_id", job.getDeptId());
        jobData.put("user", job.getUser());
        jobData.put("email", job.getEmail() != null ? job.getEmail() : "");
        jobData.put("state", job.getState().name());
        jobData.put("priority", String.valueOf(job.getPriority()));
        jobData.put("min_cores", String.valueOf(job.getMinCores()));
        jobData.put("max_cores", String.valueOf(job.getMaxCores()));
        jobData.put("min_memory", String.valueOf(job.getMinMemory()));
        jobData.put("min_gpu", String.valueOf(job.getMinGpu()));
        jobData.put("shot", job.getShot() != null ? job.getShot() : "");
        jobData.put("os", job.getOs() != null ? job.getOs() : "");
        jobData.put("uid", String.valueOf(job.getUid()));
        jobData.put("start_time", String.valueOf(job.getStartTime()));
        jobData.put("paused", String.valueOf(job.isPaused()));
        jobData.put("auto_book", String.valueOf(job.isAutoBook()));
        jobData.put("is_local", String.valueOf(job.isLocal()));
        jobData.put("max_retries", String.valueOf(job.getMaxRetries()));
        
        redisTemplate.opsForHash().putAll(jobKey, jobData);
        
        // Create indexes
        String nameIndexKey = RedisKeyBuilder.jobNameIndex(job.getName());
        redisTemplate.opsForValue().set(nameIndexKey, jobId);
        
        // Add to show's job set
        String showJobsKey = RedisKeyBuilder.showJobs(job.getShowId());
        redisTemplate.opsForSet().add(showJobsKey, jobId);
        
        // Add to dispatch queue
        updateJobDispatchQueue(job);
    }
    
    @Override
    public void updateJobState(JobInterface job, JobState state) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        
        // Atomic state update with dispatch queue management
        String script = 
            "redis.call('HSET', KEYS[1], 'state', ARGV[1]) " +
            "redis.call('HSET', KEYS[1], 'state_time', ARGV[2]) " +
            "local facility = redis.call('HGET', KEYS[1], 'facility_id') " +
            "if facility then " +
            "  if ARGV[1] == 'PENDING' then " +
            "    local priority = redis.call('HGET', KEYS[1], 'priority') " +
            "    redis.call('ZADD', KEYS[2] .. facility, priority, ARGV[3]) " +
            "  else " +
            "    redis.call('ZREM', KEYS[2] .. facility, ARGV[3]) " +
            "  end " +
            "end " +
            "return 1";
        
        redisTemplate.execute(
            new DefaultRedisScript<>(script, Long.class),
            Arrays.asList(jobKey, RedisKeyBuilder.dispatchJobsPrefix()),
            state.name(),
            String.valueOf(System.currentTimeMillis()),
            job.getJobId()
        );
    }
    
    @Override
    public void updateJobPriority(JobInterface job, int priority) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        
        redisTemplate.opsForHash().put(jobKey, "priority", String.valueOf(priority));
        
        // Update dispatch queue score
        updateJobDispatchQueue(job);
        
        // Update all frame priorities
        updateFramePriorities(job.getJobId(), priority);
    }
    
    @Override
    public boolean updateJobPaused(JobInterface job, boolean paused) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        
        String script = 
            "local current = redis.call('HGET', KEYS[1], 'paused') " +
            "if current ~= ARGV[1] then " +
            "  redis.call('HSET', KEYS[1], 'paused', ARGV[1]) " +
            "  return 1 " +
            "end " +
            "return 0";
        
        Boolean changed = redisTemplate.execute(
            new DefaultRedisScript<>(script, Boolean.class),
            Arrays.asList(jobKey),
            String.valueOf(paused)
        );
        
        if (Boolean.TRUE.equals(changed)) {
            // Update dispatch queue
            if (paused) {
                removeFromDispatchQueue(job);
            } else {
                updateJobDispatchQueue(job);
            }
        }
        
        return Boolean.TRUE.equals(changed);
    }
    
    @Override
    public void updateJobMinCores(JobInterface job, int minCores) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        redisTemplate.opsForHash().put(jobKey, "min_cores", String.valueOf(minCores));
    }
    
    @Override
    public void updateJobMaxCores(JobInterface job, int maxCores) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        redisTemplate.opsForHash().put(jobKey, "max_cores", String.valueOf(maxCores));
    }
    
    @Override
    public void updateJobMinGpu(JobInterface job, int minGpu) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        redisTemplate.opsForHash().put(jobKey, "min_gpu", String.valueOf(minGpu));
    }
    
    @Override
    public void updateJobMemory(JobInterface job, long maxRss, long usedMemory) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("max_rss", String.valueOf(maxRss));
        updates.put("used_memory", String.valueOf(usedMemory));
        
        redisTemplate.opsForHash().putAll(jobKey, updates);
    }
    
    @Override
    public void updateJobStats(JobInterface job, long totalFrames, long succeededFrames,
                               long runningFrames, long waitingFrames, long dependFrames,
                               long deadFrames, long eatenFrames) {
        String statsKey = RedisKeyBuilder.jobStats(job.getJobId());
        
        Map<String, String> stats = new HashMap<>();
        stats.put("total_frames", String.valueOf(totalFrames));
        stats.put("succeeded_frames", String.valueOf(succeededFrames));
        stats.put("running_frames", String.valueOf(runningFrames));
        stats.put("waiting_frames", String.valueOf(waitingFrames));
        stats.put("depend_frames", String.valueOf(dependFrames));
        stats.put("dead_frames", String.valueOf(deadFrames));
        stats.put("eaten_frames", String.valueOf(eatenFrames));
        stats.put("updated_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(statsKey, stats);
    }
    
    @Override
    public void deleteJob(JobInterface job) {
        String jobId = job.getJobId();
        String jobKey = RedisKeyBuilder.job(jobId);
        
        // Get job data for cleanup
        Map<Object, Object> jobData = redisTemplate.opsForHash().entries(jobKey);
        if (!jobData.isEmpty()) {
            String name = (String) jobData.get("name");
            String showId = (String) jobData.get("show_id");
            
            // Remove from indexes
            if (name != null) {
                String nameIndexKey = RedisKeyBuilder.jobNameIndex(name);
                redisTemplate.delete(nameIndexKey);
            }
            
            if (showId != null) {
                String showJobsKey = RedisKeyBuilder.showJobs(showId);
                redisTemplate.opsForSet().remove(showJobsKey, jobId);
            }
            
            // Remove from dispatch queue
            removeFromDispatchQueue(job);
        }
        
        // Delete job data
        redisTemplate.delete(jobKey);
        
        // Delete job stats
        String statsKey = RedisKeyBuilder.jobStats(jobId);
        redisTemplate.delete(statsKey);
        
        // Delete all frames
        String framePattern = RedisKeyBuilder.framePattern(jobId);
        Set<String> frameKeys = redisTemplate.keys(framePattern);
        if (frameKeys != null && !frameKeys.isEmpty()) {
            redisTemplate.delete(frameKeys);
        }
        
        // Delete all layers
        String layerPattern = RedisKeyBuilder.layerPattern(jobId);
        Set<String> layerKeys = redisTemplate.keys(layerPattern);
        if (layerKeys != null && !layerKeys.isEmpty()) {
            redisTemplate.delete(layerKeys);
        }
    }
    
    @Override
    public List<JobInterface> getJobs(JobSearchInterface request) {
        // This would require complex filtering - might keep in SQL
        // For now, return jobs from a show
        if (request.getShowId() != null) {
            String showJobsKey = RedisKeyBuilder.showJobs(request.getShowId());
            Set<String> jobIds = redisTemplate.opsForSet().members(showJobsKey);
            
            if (jobIds == null || jobIds.isEmpty()) {
                return new ArrayList<>();
            }
            
            return jobIds.stream()
                .map(this::getJob)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }
        
        return new ArrayList<>();
    }
    
    @Override
    public boolean isJobComplete(JobInterface job) {
        String statsKey = RedisKeyBuilder.jobStats(job.getJobId());
        Map<Object, Object> stats = redisTemplate.opsForHash().entries(statsKey);
        
        if (stats.isEmpty()) {
            return false;
        }
        
        long total = Long.parseLong(stats.getOrDefault("total_frames", "0").toString());
        long succeeded = Long.parseLong(stats.getOrDefault("succeeded_frames", "0").toString());
        long eaten = Long.parseLong(stats.getOrDefault("eaten_frames", "0").toString());
        
        return (succeeded + eaten) >= total;
    }
    
    private void updateJobDispatchQueue(JobInterface job) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        Map<Object, Object> jobData = redisTemplate.opsForHash().entries(jobKey);
        
        String state = (String) jobData.get("state");
        String paused = (String) jobData.get("paused");
        String facilityId = (String) jobData.get("facility_id");
        String priority = (String) jobData.get("priority");
        
        if ("PENDING".equals(state) && !"true".equals(paused) && facilityId != null) {
            String queueKey = RedisKeyBuilder.dispatchJobs(facilityId);
            double score = priority != null ? Double.parseDouble(priority) : 0.0;
            redisTemplate.opsForZSet().add(queueKey, job.getJobId(), score);
        } else {
            removeFromDispatchQueue(job);
        }
    }
    
    private void removeFromDispatchQueue(JobInterface job) {
        String jobKey = RedisKeyBuilder.job(job.getJobId());
        Object facilityId = redisTemplate.opsForHash().get(jobKey, "facility_id");
        
        if (facilityId != null) {
            String queueKey = RedisKeyBuilder.dispatchJobs(facilityId.toString());
            redisTemplate.opsForZSet().remove(queueKey, job.getJobId());
        }
    }
    
    private void updateFramePriorities(String jobId, int priority) {
        String framePattern = RedisKeyBuilder.framePattern(jobId);
        Set<String> frameKeys = redisTemplate.keys(framePattern);
        
        if (frameKeys != null && !frameKeys.isEmpty()) {
            // Batch update frame priorities
            for (String frameKey : frameKeys) {
                redisTemplate.opsForHash().put(frameKey, "priority", String.valueOf(priority));
            }
        }
    }
}

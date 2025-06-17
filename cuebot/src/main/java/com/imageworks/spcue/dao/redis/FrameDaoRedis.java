package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.FrameDao;
import com.imageworks.spcue.dao.DependDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.job.FrameState;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Profile("redis")
public class FrameDaoRedis implements FrameDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    @Autowired
    private BulkFrameUpdateScript bulkFrameUpdateScript;
    
    @Autowired
    private DependDao dependDao;
    
    @Override
    public FrameDetail getFrameDetail(String frameId) {
        String key = RedisKeyBuilder.frame(frameId);
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(key);
        
        if (frameData.isEmpty()) {
            throw new EntityNotFoundException("Frame not found: " + frameId);
        }
        
        return redisDataMapper.mapToFrameDetail(frameData);
    }
    
    @Override
    public void updateFrameState(FrameInterface frame, FrameState state) {
        String frameKey = RedisKeyBuilder.frame(frame.getFrameId());
        
        // Simple inline Lua for atomic state update with validation
        String script = 
            "local current_state = redis.call('HGET', KEYS[1], 'state') " +
            "if current_state then " +
            "  redis.call('HSET', KEYS[1], 'state', ARGV[1]) " +
            "  redis.call('HSET', KEYS[1], 'state_time', ARGV[2]) " +
            "  return 1 " +
            "end " +
            "return 0";
        
        Boolean success = redisTemplate.execute(
            new DefaultRedisScript<>(script, Boolean.class),
            Arrays.asList(frameKey),
            state.name(),
            String.valueOf(System.currentTimeMillis())
        );
        
        if (Boolean.FALSE.equals(success)) {
            throw new EntityNotFoundException("Frame not found: " + frame.getFrameId());
        }
        
        // Update dispatch queue if needed
        updateDispatchQueue(frame, state);
    }
    
    @Override
    public void updateFrameMemoryUsage(FrameInterface frame, long maxRss, long rss) {
        String key = RedisKeyBuilder.frame(frame.getFrameId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("max_rss", String.valueOf(maxRss));
        updates.put("used_memory", String.valueOf(rss));
        updates.put("memory_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(key, updates);
    }
    
    @Override
    public boolean updateFrameCleared(FrameInterface frame) {
        String frameKey = RedisKeyBuilder.frame(frame.getFrameId());
        String bookingKey = RedisKeyBuilder.booking(frame.getFrameId());
        
        // Atomic check and clear
        String script = 
            "local state = redis.call('HGET', KEYS[1], 'state') " +
            "if state == 'RUNNING' then " +
            "  redis.call('HSET', KEYS[1], 'state', 'WAITING') " +
            "  redis.call('HDEL', KEYS[1], 'proc_id', 'host_name', 'start_time') " +
            "  redis.call('DEL', KEYS[2]) " +
            "  return 1 " +
            "end " +
            "return 0";
        
        Boolean cleared = redisTemplate.execute(
            new DefaultRedisScript<>(script, Boolean.class),
            Arrays.asList(frameKey, bookingKey)
        );
        
        if (Boolean.TRUE.equals(cleared)) {
            // Re-add to dispatch queue
            updateDispatchQueue(frame, FrameState.WAITING);
        }
        
        return Boolean.TRUE.equals(cleared);
    }
    
    @Override
    public void updateFrameStarted(FrameInterface frame, ResourceUsage usage) {
        String key = RedisKeyBuilder.frame(frame.getFrameId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("state", "RUNNING");
        updates.put("start_time", String.valueOf(usage.getStartTime()));
        updates.put("retry_count", String.valueOf(frame.getRetryCount() + 1));
        
        redisTemplate.opsForHash().putAll(key, updates);
    }
    
    @Override
    public void updateFrameStopped(FrameInterface frame, FrameState state, 
                                   int exitStatus, long maxRss, long usedTime) {
        String key = RedisKeyBuilder.frame(frame.getFrameId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("state", state.name());
        updates.put("exit_status", String.valueOf(exitStatus));
        updates.put("stop_time", String.valueOf(System.currentTimeMillis()));
        updates.put("max_rss", String.valueOf(maxRss));
        updates.put("used_time", String.valueOf(usedTime));
        
        redisTemplate.opsForHash().putAll(key, updates);
        
        // Clear booking
        String bookingKey = RedisKeyBuilder.booking(frame.getFrameId());
        redisTemplate.delete(bookingKey);
        
        // If frame succeeded/eaten, mark complete for dependencies
        if (state == FrameState.SUCCEEDED || state == FrameState.EATEN) {
            dependDao.markFrameComplete(frame.getFrameId());
        }
    }
    
    @Override
    public List<FrameInterface> getStaleFrames(int cutoffSeconds) {
        // This would require a more complex query - might keep in SQL
        // or maintain a separate sorted set for frame timestamps
        long cutoffTime = System.currentTimeMillis() - (cutoffSeconds * 1000L);
        
        String staleFramesKey = RedisKeyBuilder.staleFramesQueue();
        Set<String> frameIds = redisTemplate.opsForZSet()
            .rangeByScore(staleFramesKey, 0, cutoffTime);
        
        if (frameIds == null || frameIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        return frameIds.stream()
            .map(this::getFrameDetail)
            .collect(Collectors.toList());
    }
    
    @Override
    public void markFramesWaiting(LayerInterface layer) {
        String layerId = layer.getLayerId();
        List<String> frameIds = getFrameIdsForLayer(layerId);
        
        if (!frameIds.isEmpty()) {
            // Check each frame for dependencies
            List<String> waitingFrames = new ArrayList<>();
            List<String> dependFrames = new ArrayList<>();
            
            for (String frameId : frameIds) {
                if (dependDao.satisfiesDepends(frameId)) {
                    // No dependencies or all satisfied
                    waitingFrames.add(frameId);
                } else {
                    // Has unsatisfied dependencies
                    dependFrames.add(frameId);
                }
            }
            
            // Bulk update WAITING frames
            if (!waitingFrames.isEmpty()) {
                bulkFrameUpdateScript.markFramesWaiting(
                    redisTemplate,
                    waitingFrames,
                    layer.getShowId(),
                    layer.getFacilityId()
                );
            }
            
            // Set DEPEND state for frames with dependencies
            for (String frameId : dependFrames) {
                String frameKey = RedisKeyBuilder.frame(frameId);
                redisTemplate.opsForHash().put(frameKey, "state", "DEPEND");
                redisTemplate.opsForHash().put(frameKey, "state_time", 
                    String.valueOf(System.currentTimeMillis()));
            }
        }
    }
    
    @Override
    public int retryFrames(JobInterface job, FrameState state, int maxRetries) {
        String pattern = RedisKeyBuilder.framePattern(job.getJobId());
        Set<String> frameKeys = redisTemplate.keys(pattern);
        
        if (frameKeys == null || frameKeys.isEmpty()) {
            return 0;
        }
        
        int retried = 0;
        for (String frameKey : frameKeys) {
            Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
            
            String currentState = (String) frameData.get("state");
            String retryCountStr = (String) frameData.get("retry_count");
            int retryCount = retryCountStr != null ? Integer.parseInt(retryCountStr) : 0;
            
            if (state.name().equals(currentState) && retryCount < maxRetries) {
                redisTemplate.opsForHash().put(frameKey, "state", "WAITING");
                redisTemplate.opsForHash().put(frameKey, "retry_count", "0");
                
                // Re-add to dispatch queue
                String frameId = frameKey.substring(frameKey.lastIndexOf(":") + 1);
                addToDispatchQueue(frameId, frameData);
                
                retried++;
            }
        }
        
        return retried;
    }
    
    private void updateDispatchQueue(FrameInterface frame, FrameState state) {
        String queueKey = RedisKeyBuilder.dispatchQueue(getFacilityId(frame));
        
        if (state == FrameState.WAITING) {
            // Add to queue with priority score
            double score = calculatePriorityScore(frame);
            redisTemplate.opsForZSet().add(queueKey, frame.getFrameId(), score);
        } else {
            // Remove from queue
            redisTemplate.opsForZSet().remove(queueKey, frame.getFrameId());
        }
    }
    
    private void addToDispatchQueue(String frameId, Map<Object, Object> frameData) {
        String facilityId = (String) frameData.get("facility_id");
        String queueKey = RedisKeyBuilder.dispatchQueue(facilityId);
        
        String priority = (String) frameData.get("priority");
        double score = priority != null ? Double.parseDouble(priority) : 0.0;
        
        redisTemplate.opsForZSet().add(queueKey, frameId, score);
    }
    
    private double calculatePriorityScore(FrameInterface frame) {
        // Priority calculation logic
        return frame.getPriority() * 10000 + 
               (100 - frame.getLayerOrder()) * 100 + 
               (10000 - frame.getFrameNumber());
    }
    
    private String getFacilityId(FrameInterface frame) {
        String frameKey = RedisKeyBuilder.frame(frame.getFrameId());
        Object facilityId = redisTemplate.opsForHash().get(frameKey, "facility_id");
        return facilityId != null ? facilityId.toString() : "default";
    }
    
    private List<String> getFrameIdsForLayer(String layerId) {
        String pattern = RedisKeyBuilder.framePatternByLayer(layerId);
        Set<String> frameKeys = redisTemplate.keys(pattern);
        
        if (frameKeys == null || frameKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        return frameKeys.stream()
            .map(key -> key.substring(key.lastIndexOf(":") + 1))
            .collect(Collectors.toList());
    }
}

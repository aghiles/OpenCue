package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class BulkFrameUpdateScript {
    
    private static final String SCRIPT_PATH = "dao/redis/scripts/bulk_frame_update.lua";
    
    private RedisScript<Long> script;
    
    @PostConstruct
    public void init() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(loadScript());
        redisScript.setResultType(Long.class);
        this.script = redisScript;
    }
    
    private String loadScript() {
        try {
            ClassPathResource resource = new ClassPathResource(SCRIPT_PATH);
            try (Reader reader = new InputStreamReader(resource.getInputStream())) {
                return FileCopyUtils.copyToString(reader);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Lua script: " + SCRIPT_PATH, e);
        }
    }
    
    /**
     * Bulk update frames to WAITING state and add to dispatch queue
     * 
     * @param template Redis template for execution
     * @param frameIds List of frame IDs to update
     * @param showId Show ID for the frames
     * @param facilityId Facility ID for dispatch queue
     * @return Number of frames updated
     */
    public long markFramesWaiting(
            RedisTemplate<String, String> template,
            List<String> frameIds,
            String showId,
            String facilityId) {
        
        List<String> keys = Arrays.asList(
            RedisKeyBuilder.framePrefix(),              // KEYS[1]
            RedisKeyBuilder.dispatchQueue(facilityId),  // KEYS[2]
            RedisKeyBuilder.bookingPrefix()             // KEYS[3]
        );
        
        List<Object> args = new ArrayList<>();
        args.add("WAITING");                            // ARGV[1] - new state
        args.add(String.valueOf(System.currentTimeMillis())); // ARGV[2] - timestamp
        args.addAll(frameIds);                          // ARGV[3+] - frame IDs
        
        return template.execute(script, keys, args.toArray());
    }
    
    /**
     * Bulk update frame states
     * 
     * @param template Redis template for execution
     * @param frameIds List of frame IDs to update
     * @param newState New state for all frames
     * @return Number of frames updated
     */
    public long updateFrameStates(
            RedisTemplate<String, String> template,
            List<String> frameIds,
            String newState) {
        
        // Inline script for simple state updates
        String stateScript = 
            "local updated = 0 " +
            "local timestamp = ARGV[2] " +
            "for i = 3, #ARGV do " +
            "  local frame_key = KEYS[1] .. ARGV[i] " +
            "  if redis.call('EXISTS', frame_key) == 1 then " +
            "    redis.call('HSET', frame_key, 'state', ARGV[1]) " +
            "    redis.call('HSET', frame_key, 'state_time', timestamp) " +
            "    updated = updated + 1 " +
            "  end " +
            "end " +
            "return updated";
        
        List<Object> args = new ArrayList<>();
        args.add(newState);
        args.add(String.valueOf(System.currentTimeMillis()));
        args.addAll(frameIds);
        
        return template.execute(
            new DefaultRedisScript<>(stateScript, Long.class),
            Arrays.asList(RedisKeyBuilder.framePrefix()),
            args.toArray()
        );
    }
    
    /**
     * Kill all frames in a job
     * 
     * @param template Redis template for execution
     * @param jobId Job ID
     * @param facilityId Facility ID for dispatch queue cleanup
     * @return Number of frames killed
     */
    public long killJobFrames(
            RedisTemplate<String, String> template,
            String jobId,
            String facilityId) {
        
        String killScript = 
            "local killed = 0 " +
            "local pattern = KEYS[1] .. ARGV[1] .. ':*' " +
            "local frame_keys = redis.call('KEYS', pattern) " +
            "local timestamp = ARGV[2] " +
            "for _, frame_key in ipairs(frame_keys) do " +
            "  local state = redis.call('HGET', frame_key, 'state') " +
            "  if state == 'WAITING' or state == 'RUNNING' then " +
            "    redis.call('HSET', frame_key, 'state', 'DEAD') " +
            "    redis.call('HSET', frame_key, 'state_time', timestamp) " +
            "    redis.call('HSET', frame_key, 'exit_status', '-1') " +
            "    local frame_id = string.sub(frame_key, string.len(KEYS[1]) + 1) " +
            "    redis.call('ZREM', KEYS[2], frame_id) " +
            "    redis.call('DEL', KEYS[3] .. frame_id) " +
            "    killed = killed + 1 " +
            "  end " +
            "end " +
            "return killed";
        
        return template.execute(
            new DefaultRedisScript<>(killScript, Long.class),
            Arrays.asList(
                RedisKeyBuilder.framePrefix(),
                RedisKeyBuilder.dispatchQueue(facilityId),
                RedisKeyBuilder.bookingPrefix()
            ),
            jobId,
            String.valueOf(System.currentTimeMillis())
        );
    }
    
    /**
     * Retry failed frames
     * 
     * @param template Redis template for execution
     * @param frameIds List of frame IDs to retry
     * @param facilityId Facility ID for dispatch queue
     * @param maxRetries Maximum retry count
     * @return Number of frames retried
     */
    public long retryFrames(
            RedisTemplate<String, String> template,
            List<String> frameIds,
            String facilityId,
            int maxRetries) {
        
        String retryScript = 
            "local retried = 0 " +
            "local timestamp = ARGV[1] " +
            "local max_retries = tonumber(ARGV[2]) " +
            "for i = 3, #ARGV do " +
            "  local frame_key = KEYS[1] .. ARGV[i] " +
            "  local frame_data = redis.call('HGETALL', frame_key) " +
            "  if #frame_data > 0 then " +
            "    local frame = {} " +
            "    for j = 1, #frame_data, 2 do " +
            "      frame[frame_data[j]] = frame_data[j + 1] " +
            "    end " +
            "    local retry_count = tonumber(frame['retry_count'] or '0') " +
            "    if frame['state'] == 'DEAD' and retry_count < max_retries then " +
            "      redis.call('HMSET', frame_key, " +
            "        'state', 'WAITING', " +
            "        'state_time', timestamp, " +
            "        'retry_count', retry_count + 1) " +
            "      redis.call('HDEL', frame_key, 'exit_status', 'stop_time') " +
            "      local priority = tonumber(frame['priority'] or '0') " +
            "      redis.call('ZADD', KEYS[2], priority, ARGV[i]) " +
            "      retried = retried + 1 " +
            "    end " +
            "  end " +
            "end " +
            "return retried";
        
        List<Object> args = new ArrayList<>();
        args.add(String.valueOf(System.currentTimeMillis()));
        args.add(String.valueOf(maxRetries));
        args.addAll(frameIds);
        
        return template.execute(
            new DefaultRedisScript<>(retryScript, Long.class),
            Arrays.asList(
                RedisKeyBuilder.framePrefix(),
                RedisKeyBuilder.dispatchQueue(facilityId)
            ),
            args.toArray()
        );
    }
    
    /**
     * Update resource requirements for all frames in a layer
     * 
     * @param template Redis template for execution
     * @param layerId Layer ID
     * @param cores New core requirement
     * @param memory New memory requirement
     * @param gpu New GPU requirement
     * @return Number of frames updated
     */
    public long updateLayerFrameRequirements(
            RedisTemplate<String, String> template,
            String layerId,
            int cores,
            long memory,
            int gpu) {
        
        String updateScript = 
            "local updated = 0 " +
            "local pattern = KEYS[1] .. '*:' .. ARGV[1] .. ':*' " +
            "local frame_keys = redis.call('KEYS', pattern) " +
            "for _, frame_key in ipairs(frame_keys) do " +
            "  local state = redis.call('HGET', frame_key, 'state') " +
            "  if state == 'WAITING' then " +
            "    redis.call('HMSET', frame_key, " +
            "      'cores_min', ARGV[2], " +
            "      'memory_min', ARGV[3], " +
            "      'gpu_min', ARGV[4]) " +
            "    updated = updated + 1 " +
            "  end " +
            "end " +
            "return updated";
        
        return template.execute(
            new DefaultRedisScript<>(updateScript, Long.class),
            Arrays.asList(RedisKeyBuilder.framePrefix()),
            layerId,
            String.valueOf(cores),
            String.valueOf(memory),
            String.valueOf(gpu)
        );
    }
}

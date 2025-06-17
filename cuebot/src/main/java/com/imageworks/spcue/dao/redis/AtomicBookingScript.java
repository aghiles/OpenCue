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
import java.util.Arrays;

@Component
public class AtomicBookingScript {
    
    private static final String SCRIPT_PATH = "dao/redis/scripts/atomic_booking.lua";
    
    private RedisScript<Boolean> script;
    
    @PostConstruct
    public void init() {
        DefaultRedisScript<Boolean> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(loadScript());
        redisScript.setResultType(Boolean.class);
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
     * Atomically book a frame to a proc/host
     * 
     * @param template Redis template for execution
     * @param frameId Frame to book
     * @param procId Proc that will run the frame
     * @param hostId Host running the proc
     * @return true if booking succeeded, false if frame was already booked
     */
    public boolean bookFrame(
            RedisTemplate<String, String> template,
            String frameId,
            String procId,
            String hostId) {
        
        return template.execute(
            script,
            Arrays.asList(
                RedisKeyBuilder.frame(frameId),         // KEYS[1]
                RedisKeyBuilder.booking(frameId),       // KEYS[2]
                RedisKeyBuilder.proc(procId),           // KEYS[3]
                RedisKeyBuilder.dispatchQueuePrefix()   // KEYS[4]
            ),
            frameId,
            procId,
            hostId,
            String.valueOf(System.currentTimeMillis())
        );
    }
    
    /**
     * Atomically book multiple frames in a batch
     * 
     * @param template Redis template for execution
     * @param frameIds List of frame IDs to book
     * @param procId Proc that will run the frames
     * @param hostId Host running the proc
     * @return Number of successfully booked frames
     */
    public int bookFrames(
            RedisTemplate<String, String> template,
            String[] frameIds,
            String procId,
            String hostId) {
        
        // For batch booking, use inline script for simplicity
        String batchScript = 
            "local booked = 0 " +
            "local timestamp = ARGV[#ARGV] " +
            "for i = 1, #ARGV - 3 do " +
            "  local frame_id = ARGV[i] " +
            "  local frame_key = KEYS[1] .. frame_id " +
            "  local booking_key = KEYS[2] .. frame_id " +
            "  if redis.call('EXISTS', booking_key) == 0 then " +
            "    local state = redis.call('HGET', frame_key, 'state') " +
            "    if state == 'WAITING' then " +
            "      redis.call('SETEX', booking_key, 86400, ARGV[#ARGV-2]) " +
            "      redis.call('HMSET', frame_key, " +
            "        'state', 'RUNNING', " +
            "        'proc_id', ARGV[#ARGV-2], " +
            "        'host_id', ARGV[#ARGV-1], " +
            "        'start_time', timestamp) " +
            "      booked = booked + 1 " +
            "    end " +
            "  end " +
            "end " +
            "return booked";
        
        // Build arguments array
        Object[] args = new Object[frameIds.length + 3];
        System.arraycopy(frameIds, 0, args, 0, frameIds.length);
        args[frameIds.length] = procId;
        args[frameIds.length + 1] = hostId;
        args[frameIds.length + 2] = String.valueOf(System.currentTimeMillis());
        
        return template.execute(
            new DefaultRedisScript<>(batchScript, Integer.class),
            Arrays.asList(
                RedisKeyBuilder.framePrefix(),
                RedisKeyBuilder.bookingPrefix()
            ),
            args
        );
    }
    
    /**
     * Release a booking atomically
     * 
     * @param template Redis template for execution
     * @param frameId Frame to release
     * @param facilityId Facility ID for re-adding to dispatch queue
     * @return true if booking was released, false if no booking existed
     */
    public boolean releaseBooking(
            RedisTemplate<String, String> template,
            String frameId,
            String facilityId) {
        
        String releaseScript = 
            "local booking_exists = redis.call('EXISTS', KEYS[2]) " +
            "if booking_exists == 1 then " +
            "  redis.call('DEL', KEYS[2]) " +
            "  local state = redis.call('HGET', KEYS[1], 'state') " +
            "  if state == 'RUNNING' then " +
            "    redis.call('HSET', KEYS[1], 'state', 'WAITING') " +
            "    redis.call('HDEL', KEYS[1], 'proc_id', 'host_id', 'start_time') " +
            "    local priority = redis.call('HGET', KEYS[1], 'priority') " +
            "    if priority then " +
            "      redis.call('ZADD', KEYS[3], priority, ARGV[1]) " +
            "    end " +
            "  end " +
            "  return 1 " +
            "end " +
            "return 0";
        
        return template.execute(
            new DefaultRedisScript<>(releaseScript, Boolean.class),
            Arrays.asList(
                RedisKeyBuilder.frame(frameId),
                RedisKeyBuilder.booking(frameId),
                RedisKeyBuilder.dispatchQueue(facilityId)
            ),
            frameId
        );
    }
    
    /**
     * Check if a frame is booked
     * 
     * @param template Redis template for execution
     * @param frameId Frame to check
     * @return true if frame is booked, false otherwise
     */
    public boolean isBooked(RedisTemplate<String, String> template, String frameId) {
        String bookingKey = RedisKeyBuilder.booking(frameId);
        return template.hasKey(bookingKey);
    }
    
    /**
     * Get booking details for a frame
     * 
     * @param template Redis template for execution
     * @param frameId Frame to check
     * @return Proc ID if booked, null otherwise
     */
    public String getBookingProcId(RedisTemplate<String, String> template, String frameId) {
        String bookingKey = RedisKeyBuilder.booking(frameId);
        return template.opsForValue().get(bookingKey);
    }
}

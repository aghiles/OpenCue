package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.DispatchFrame;

import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.stream.Collectors;

@Component
public class FindDispatchableFrames {
    
    private static final String SCRIPT_PATH = "dao/redis/scripts/find_dispatchable_frames.lua";
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    private RedisScript<List> script;
    
    @PostConstruct
    public void init() {
        DefaultRedisScript<List> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(loadScript());
        redisScript.setResultType(List.class);
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
     * Find frames that are ready for dispatch based on resource requirements
     * 
     * @param template Redis template for execution
     * @param facilityId Facility to search within
     * @param cores Available CPU cores
     * @param memory Available memory in KB
     * @param gpus Available GPU units
     * @param services Array of available services on the host
     * @return List of dispatchable frames, ordered by priority
     */
    public List<DispatchFrame> execute(
            RedisTemplate<String, String> template,
            String facilityId, 
            int cores, 
            long memory, 
            int gpus, 
            String[] services) {
        
        return execute(template, facilityId, cores, memory, gpus, services, 100);
    }
    
    /**
     * Find frames that are ready for dispatch with custom limit
     * 
     * @param template Redis template for execution
     * @param facilityId Facility to search within
     * @param cores Available CPU cores
     * @param memory Available memory in KB
     * @param gpus Available GPU units
     * @param services Array of available services on the host
     * @param limit Maximum number of frames to return
     * @return List of dispatchable frames, ordered by priority
     */
    public List<DispatchFrame> execute(
            RedisTemplate<String, String> template,
            String facilityId, 
            int cores, 
            long memory, 
            int gpus, 
            String[] services,
            int limit) {
        
        // Prepare Redis keys
        List<String> keys = Arrays.asList(
            RedisKeyBuilder.dispatchQueue(facilityId),      // KEYS[1]
            RedisKeyBuilder.bookingPrefix(),                // KEYS[2]
            RedisKeyBuilder.framePrefix(),                  // KEYS[3]
            RedisKeyBuilder.layerServicePrefix()            // KEYS[4]
        );
        
        // Prepare arguments
        List<Object> args = new ArrayList<>();
        args.add(String.valueOf(cores));                    // ARGV[1]
        args.add(String.valueOf(memory));                   // ARGV[2]
        args.add(String.valueOf(gpus));                     // ARGV[3]
        args.add(String.valueOf(limit));                    // ARGV[4]
        
        // Add available services
        if (services != null) {
            for (String service : services) {
                args.add(service);                          // ARGV[5+]
            }
        }
        
        // Execute script - returns list of JSON strings
        @SuppressWarnings("unchecked")
        List<String> jsonResults = template.execute(
            script, 
            keys, 
            args.toArray()
        );
        
        // Convert JSON results to DispatchFrame objects
        return jsonResults.stream()
            .map(redisDataMapper::jsonToDispatchFrame)
            .collect(Collectors.toList());
    }
    
    /**
     * Execute with additional filters
     */
    public List<DispatchFrame> execute(
            RedisTemplate<String, String> template,
            String facilityId, 
            int cores, 
            long memory, 
            int gpus, 
            String[] services,
            int limit,
            String showId,
            String groupId) {
        
        // Extended version with show/group filtering
        List<String> keys = Arrays.asList(
            RedisKeyBuilder.dispatchQueue(facilityId),
            RedisKeyBuilder.bookingPrefix(),
            RedisKeyBuilder.framePrefix(),
            RedisKeyBuilder.layerServicePrefix()
        );
        
        List<Object> args = new ArrayList<>();
        args.add(String.valueOf(cores));
        args.add(String.valueOf(memory));
        args.add(String.valueOf(gpus));
        args.add(String.valueOf(limit));
        
        if (services != null) {
            for (String service : services) {
                args.add(service);
            }
        }
        
        // Add optional filters
        if (showId != null) {
            args.add("show_id:" + showId);
        }
        if (groupId != null) {
            args.add("group_id:" + groupId);
        }
        
        @SuppressWarnings("unchecked")
        List<String> jsonResults = template.execute(
            script, 
            keys, 
            args.toArray()
        );
        
        return jsonResults.stream()
            .map(redisDataMapper::jsonToDispatchFrame)
            .collect(Collectors.toList());
    }
    
    /**
     * Get script SHA for monitoring/debugging
     */
    public String getScriptSha() {
        return script.getSha1();
    }
    
    /**
     * Preload script to Redis for better performance
     */
    public void preloadScript(RedisTemplate<String, String> template) {
        template.execute((RedisScript<String>) connection -> {
            return connection.scriptLoad(script.getScriptAsString());
        });
    }
}

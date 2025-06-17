package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.DependDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.depend.Depend;
import com.imageworks.spcue.depend.DependType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;

@Repository
@Profile("redis")
public class DependencyDaoRedis implements DependDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Override
    public void insertDepend(Depend depend) {
        String dependKey = RedisKeyBuilder.depend(depend.getId());
        
        Map<String, String> dependData = new HashMap<>();
        dependData.put("id", depend.getId());
        dependData.put("type", depend.getType().name());
        dependData.put("active", String.valueOf(depend.isActive()));
        dependData.put("any_frame", String.valueOf(depend.isAnyFrame()));
        
        // Store what depends on what
        if (depend.getDependOnJobId() != null) {
            dependData.put("depend_on_job", depend.getDependOnJobId());
        }
        if (depend.getDependOnLayerId() != null) {
            dependData.put("depend_on_layer", depend.getDependOnLayerId());
        }
        if (depend.getDependOnFrameId() != null) {
            dependData.put("depend_on_frame", depend.getDependOnFrameId());
        }
        
        // Store what is dependent
        if (depend.getDependErJobId() != null) {
            dependData.put("depend_er_job", depend.getDependErJobId());
        }
        if (depend.getDependErLayerId() != null) {
            dependData.put("depend_er_layer", depend.getDependErLayerId());
        }
        if (depend.getDependErFrameId() != null) {
            dependData.put("depend_er_frame", depend.getDependErFrameId());
        }
        
        redisTemplate.opsForHash().putAll(dependKey, dependData);
        
        // Add to dependency sets for fast lookup
        addToDependencySets(depend);
    }
    
    private void addToDependencySets(Depend depend) {
        // For frame dependencies, maintain sets for fast checking
        if (depend.getDependErFrameId() != null) {
            String frameDepKey = RedisKeyBuilder.frameDependencies(depend.getDependErFrameId());
            
            if (depend.getDependOnFrameId() != null) {
                redisTemplate.opsForSet().add(frameDepKey, depend.getDependOnFrameId());
            } else if (depend.getDependOnLayerId() != null) {
                // Frame depends on all frames in layer
                String layerFramesKey = RedisKeyBuilder.layerFrames(depend.getDependOnLayerId());
                Set<String> layerFrames = redisTemplate.opsForSet().members(layerFramesKey);
                if (layerFrames != null && !layerFrames.isEmpty()) {
                    redisTemplate.opsForSet().add(frameDepKey, layerFrames.toArray(new String[0]));
                }
            } else if (depend.getDependOnJobId() != null) {
                // Frame depends on all frames in job
                String jobFramesKey = RedisKeyBuilder.jobFrames(depend.getDependOnJobId());
                Set<String> jobFrames = redisTemplate.opsForSet().members(jobFramesKey);
                if (jobFrames != null && !jobFrames.isEmpty()) {
                    redisTemplate.opsForSet().add(frameDepKey, jobFrames.toArray(new String[0]));
                }
            }
        }
    }
    
    @Override
    public boolean satisfiesDepends(String frameId) {
        String luaScript = 
            "local frame_deps_key = KEYS[1]\n" +
            "local frame_prefix = KEYS[2]\n" +
            "\n" +
            "-- Get all frames this frame depends on\n" +
            "local depends_on = redis.call('SMEMBERS', frame_deps_key)\n" +
            "if #depends_on == 0 then\n" +
            "  return 1  -- No dependencies\n" +
            "end\n" +
            "\n" +
            "-- Check each dependency\n" +
            "for _, dep_frame_id in ipairs(depends_on) do\n" +
            "  local frame_key = frame_prefix .. dep_frame_id\n" +
            "  local state = redis.call('HGET', frame_key, 'state')\n" +
            "  if state ~= 'SUCCEEDED' and state ~= 'EATEN' then\n" +
            "    return 0  -- Dependency not satisfied\n" +
            "  end\n" +
            "end\n" +
            "\n" +
            "return 1  -- All dependencies satisfied";
        
        Boolean satisfied = redisTemplate.execute(
            new DefaultRedisScript<>(luaScript, Boolean.class),
            Arrays.asList(
                RedisKeyBuilder.frameDependencies(frameId),
                RedisKeyBuilder.framePrefix()
            )
        );
        
        return Boolean.TRUE.equals(satisfied);
    }
    
    @Override
    public void markFrameComplete(String frameId) {
        // When a frame completes, update dependent frames
        String luaScript = 
            "local completed_frame_id = ARGV[1]\n" +
            "local dependents_key = KEYS[1]\n" +
            "\n" +
            "-- Get all frames that depend on this completed frame\n" +
            "local dependents = redis.call('SMEMBERS', dependents_key)\n" +
            "\n" +
            "for _, dependent_frame_id in ipairs(dependents) do\n" +
            "  -- Remove from the dependent's dependency set\n" +
            "  local dep_key = KEYS[2] .. dependent_frame_id\n" +
            "  redis.call('SREM', dep_key, completed_frame_id)\n" +
            "end\n" +
            "\n" +
            "return #dependents";
        
        redisTemplate.execute(
            new DefaultRedisScript<>(luaScript, Long.class),
            Arrays.asList(
                RedisKeyBuilder.frameDependents(frameId),
                RedisKeyBuilder.frameDependenciesPrefix()
            ),
            frameId
        );
    }
}

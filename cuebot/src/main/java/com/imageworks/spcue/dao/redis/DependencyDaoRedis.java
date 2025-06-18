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
        
        // Add to dependency sets based on type
        addToDependencySets(depend);
    }
    
    private void addToDependencySets(Depend depend) {
        DependType type = depend.getType();
        
        // Job-level dependencies
        if (type == DependType.JOB_ON_JOB) {
            String jobDepsKey = RedisKeyBuilder.jobDependencies(depend.getDependErJobId());
            redisTemplate.opsForSet().add(jobDepsKey, depend.getDependOnJobId());
            
            String jobDependentsKey = RedisKeyBuilder.jobDependents(depend.getDependOnJobId());
            redisTemplate.opsForSet().add(jobDependentsKey, depend.getDependErJobId());
        }
        
        // Layer-level dependencies
        else if (type == DependType.LAYER_ON_LAYER || 
                 type == DependType.LAYER_ON_JOB ||
                 type == DependType.JOB_ON_LAYER) {
            
            String layerId = depend.getDependErLayerId();
            if (layerId == null && depend.getDependErJobId() != null) {
                // JOB_ON_LAYER - all layers in job depend on target layer
                // For now, we'll handle at job level
                return;
            }
            
            if (layerId != null) {
                String layerDepsKey = RedisKeyBuilder.layerDependencies(layerId);
                
                if (depend.getDependOnLayerId() != null) {
                    redisTemplate.opsForSet().add(layerDepsKey, depend.getDependOnLayerId());
                    
                    String layerDependentsKey = RedisKeyBuilder.layerDependents(depend.getDependOnLayerId());
                    redisTemplate.opsForSet().add(layerDependentsKey, layerId);
                } else if (depend.getDependOnJobId() != null) {
                    // Layer depends on entire job
                    redisTemplate.opsForSet().add(layerDepsKey, "job:" + depend.getDependOnJobId());
                }
                
                // Mark layer as blocked initially
                String layerKey = RedisKeyBuilder.layer(layerId);
                redisTemplate.opsForHash().put(layerKey, "active", "false");
                redisTemplate.opsForHash().put(layerKey, "blocked_by_depends", "true");
            }
        }
        
        // Frame-level dependencies (only for specific frame-on-frame)
        else if (type == DependType.FRAME_ON_FRAME && 
                 depend.getDependErFrameId() != null && 
                 depend.getDependOnFrameId() != null) {
            
            String frameDepsKey = RedisKeyBuilder.frameDependencies(depend.getDependErFrameId());
            redisTemplate.opsForSet().add(frameDepsKey, depend.getDependOnFrameId());
            
            String frameDependentsKey = RedisKeyBuilder.frameDependents(depend.getDependOnFrameId());
            redisTemplate.opsForSet().add(frameDependentsKey, depend.getDependErFrameId());
        }
    }
    
    @Override
    public boolean satisfiesDepends(String frameId) {
        // First check layer-level dependencies
        String frameKey = RedisKeyBuilder.frame(frameId);
        String layerId = (String) redisTemplate.opsForHash().get(frameKey, "layer_id");
        
        if (layerId != null) {
            String layerKey = RedisKeyBuilder.layer(layerId);
            String layerActive = (String) redisTemplate.opsForHash().get(layerKey, "active");
            
            if (!"true".equals(layerActive)) {
                // Layer not active, frame cannot run
                return false;
            }
        }
        
        // Then check frame-specific dependencies
        String luaScript = 
            "local frame_deps_key = KEYS[1]\n" +
            "local frame_prefix = KEYS[2]\n" +
            "\n" +
            "-- Get all frames this frame depends on\n" +
            "local depends_on = redis.call('SMEMBERS', frame_deps_key)\n" +
            "if #depends_on == 0 then\n" +
            "  return 1  -- No frame dependencies\n" +
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
        // Update frame dependents
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
        
        // Check if this completes a layer
        checkLayerCompletion(frameId);
    }
    
    public boolean checkLayerDependencies(String layerId) {
        String luaScript = 
            "local layer_deps_key = KEYS[1]\n" +
            "local layer_prefix = KEYS[2]\n" +
            "local job_prefix = KEYS[3]\n" +
            "\n" +
            "local dependencies = redis.call('SMEMBERS', layer_deps_key)\n" +
            "if #dependencies == 0 then\n" +
            "  return 1  -- No dependencies\n" +
            "end\n" +
            "\n" +
            "for _, dep_id in ipairs(dependencies) do\n" +
            "  if string.sub(dep_id, 1, 4) == 'job:' then\n" +
            "    -- Job dependency\n" +
            "    local job_id = string.sub(dep_id, 5)\n" +
            "    local job_state = redis.call('HGET', job_prefix .. job_id, 'state')\n" +
            "    if job_state ~= 'FINISHED' then\n" +
            "      return 0\n" +
            "    end\n" +
            "  else\n" +
            "    -- Layer dependency\n" +
            "    local layer_complete = redis.call('HGET', layer_prefix .. dep_id, 'is_complete')\n" +
            "    if layer_complete ~= 'true' then\n" +
            "      return 0\n" +
            "    end\n" +
            "  end\n" +
            "end\n" +
            "\n" +
            "return 1";
        
        Boolean satisfied = redisTemplate.execute(
            new DefaultRedisScript<>(luaScript, Boolean.class),
            Arrays.asList(
                RedisKeyBuilder.layerDependencies(layerId),
                RedisKeyBuilder.layerPrefix(),
                RedisKeyBuilder.jobPrefix()
            )
        );
        
        return Boolean.TRUE.equals(satisfied);
    }
    
    public boolean checkJobDependencies(String jobId) {
        String luaScript = 
            "local job_deps_key = KEYS[1]\n" +
            "local job_prefix = KEYS[2]\n" +
            "\n" +
            "local dependencies = redis.call('SMEMBERS', job_deps_key)\n" +
            "if #dependencies == 0 then\n" +
            "  return 1  -- No dependencies\n" +
            "end\n" +
            "\n" +
            "for _, dep_job_id in ipairs(dependencies) do\n" +
            "  local job_state = redis.call('HGET', job_prefix .. dep_job_id, 'state')\n" +
            "  if job_state ~= 'FINISHED' then\n" +
            "    return 0\n" +
            "  end\n" +
            "end\n" +
            "\n" +
            "return 1";
        
        Boolean satisfied = redisTemplate.execute(
            new DefaultRedisScript<>(luaScript, Boolean.class),
            Arrays.asList(
                RedisKeyBuilder.jobDependencies(jobId),
                RedisKeyBuilder.jobPrefix()
            )
        );
        
        return Boolean.TRUE.equals(satisfied);
    }
    
    private void checkLayerCompletion(String frameId) {
        // Get frame's layer
        String frameKey = RedisKeyBuilder.frame(frameId);
        String layerId = (String) redisTemplate.opsForHash().get(frameKey, "layer_id");
        
        if (layerId != null) {
            // Check if all frames in layer are complete
            String statsKey = RedisKeyBuilder.layerStats(layerId);
            Map<Object, Object> stats = redisTemplate.opsForHash().entries(statsKey);
            
            if (!stats.isEmpty()) {
                long total = Long.parseLong(stats.getOrDefault("total_frames", "0").toString());
                long succeeded = Long.parseLong(stats.getOrDefault("succeeded_frames", "0").toString());
                long eaten = Long.parseLong(stats.getOrDefault("eaten_frames", "0").toString());
                
                if ((succeeded + eaten) >= total && total > 0) {
                    onLayerComplete(layerId);
                }
            }
        }
    }
    
    public void onLayerComplete(String completedLayerId) {
        // Mark layer as complete
        String layerKey = RedisKeyBuilder.layer(completedLayerId);
        redisTemplate.opsForHash().put(layerKey, "is_complete", "true");
        
        // Find and try to activate dependent layers
        String dependentsKey = RedisKeyBuilder.layerDependents(completedLayerId);
        Set<String> dependentLayers = redisTemplate.opsForSet().members(dependentsKey);
        
        if (dependentLayers != null) {
            for (String layerId : dependentLayers) {
                tryActivateLayer(layerId);
            }
        }
        
        // Check if this completes a job
        String jobId = (String) redisTemplate.opsForHash().get(layerKey, "job_id");
        if (jobId != null) {
            checkJobCompletion(jobId);
        }
    }
    
    public void onJobComplete(String completedJobId) {
        // Mark job as complete
        String jobKey = RedisKeyBuilder.job(completedJobId);
        redisTemplate.opsForHash().put(jobKey, "state", "FINISHED");
        
        // Find and try to activate dependent jobs
        String dependentsKey = RedisKeyBuilder.jobDependents(completedJobId);
        Set<String> dependentJobs = redisTemplate.opsForSet().members(dependentsKey);
        
        if (dependentJobs != null) {
            for (String jobId : dependentJobs) {
                tryActivateJob(jobId);
            }
        }
    }
    
    private void tryActivateLayer(String layerId) {
        if (checkLayerDependencies(layerId)) {
            // Activate the layer
            String layerKey = RedisKeyBuilder.layer(layerId);
            redisTemplate.opsForHash().put(layerKey, "active", "true");
            redisTemplate.opsForHash().put(layerKey, "blocked_by_depends", "false");
            
            // This will trigger frame activation in the next dispatch cycle
            // The dispatcher will see the layer is now active
        }
    }
    
    private void tryActivateJob(String jobId) {
        if (checkJobDependencies(jobId)) {
            // Activate all layers in the job
            String jobLayersKey = RedisKeyBuilder.jobLayers(jobId);
            Set<String> layerIds = redisTemplate.opsForZSet().range(jobLayersKey, 0, -1);
            
            if (layerIds != null) {
                for (String layerId : layerIds) {
                    tryActivateLayer(layerId);
                }
            }
        }
    }
    
    private void checkJobCompletion(String jobId) {
        // Similar to JobDaoRedis.isJobComplete() but triggers events
        String statsKey = RedisKeyBuilder.jobStats(jobId);
        Map<Object, Object> stats = redisTemplate.opsForHash().entries(statsKey);
        
        if (!stats.isEmpty()) {
            long total = Long.parseLong(stats.getOrDefault("total_frames", "0").toString());
            long succeeded = Long.parseLong(stats.getOrDefault("succeeded_frames", "0").toString());
            long eaten = Long.parseLong(stats.getOrDefault("eaten_frames", "0").toString());
            
            if ((succeeded + eaten) >= total && total > 0) {
                onJobComplete(jobId);
            }
        }
    }
}

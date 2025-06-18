package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.LayerDao;
import com.imageworks.spcue.dao.DependDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.job.LayerType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Profile("redis")
public class LayerDaoRedis implements LayerDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    @Autowired
    private DependDao dependDao;
    
    @Override
    public LayerDetail getLayerDetail(String id) {
        String key = RedisKeyBuilder.layer(id);
        Map<Object, Object> layerData = redisTemplate.opsForHash().entries(key);
        
        if (layerData.isEmpty()) {
            throw new EntityNotFoundException("Layer not found: " + id);
        }
        
        return redisDataMapper.mapToLayerDetail(layerData);
    }
    
    @Override
    public LayerInterface getLayer(String id) {
        return getLayerDetail(id);
    }
    
    @Override
    public LayerInterface findLayer(JobInterface job, String name) {
        String indexKey = RedisKeyBuilder.layerNameIndex(job.getJobId(), name);
        String layerId = redisTemplate.opsForValue().get(indexKey);
        
        if (layerId == null) {
            return null;
        }
        
        return getLayer(layerId);
    }
    
    @Override
    public void insertLayer(LayerDetail layer) {
        String layerId = layer.getLayerId();
        String layerKey = RedisKeyBuilder.layer(layerId);
        
        Map<String, String> layerData = new HashMap<>();
        layerData.put("id", layerId);
        layerData.put("job_id", layer.getJobId());
        layerData.put("name", layer.getName());
        layerData.put("type", layer.getType().name());
        layerData.put("range", layer.getRange());
        layerData.put("chunk_size", String.valueOf(layer.getChunkSize()));
        layerData.put("dispatch_order", String.valueOf(layer.getDispatchOrder()));
        layerData.put("cores_min", String.valueOf(layer.getMinCores()));
        layerData.put("cores_max", String.valueOf(layer.getMaxCores()));
        layerData.put("memory_min", String.valueOf(layer.getMinMemory()));
        layerData.put("gpu_min", String.valueOf(layer.getMinGpu()));
        layerData.put("timeout", String.valueOf(layer.getTimeout()));
        layerData.put("timeout_llu", String.valueOf(layer.getTimeoutLLU()));
        layerData.put("is_threadable", String.valueOf(layer.isThreadable()));
        layerData.put("enabled", String.valueOf(true));
        layerData.put("command", layer.getCommand());
        
        // NEW: Set layer as inactive by default (will be activated when dependencies are met)
        layerData.put("active", "false");
        layerData.put("is_complete", "false");
        
        redisTemplate.opsForHash().putAll(layerKey, layerData);
        
        // Create indexes
        String nameIndexKey = RedisKeyBuilder.layerNameIndex(layer.getJobId(), layer.getName());
        redisTemplate.opsForValue().set(nameIndexKey, layerId);
        
        // Add to job's layer set
        String jobLayersKey = RedisKeyBuilder.jobLayers(layer.getJobId());
        redisTemplate.opsForZSet().add(jobLayersKey, layerId, layer.getDispatchOrder());
        
        // Store services if any
        if (layer.getServices() != null && layer.getServices().length > 0) {
            String servicesKey = RedisKeyBuilder.layerServices(layerId);
            redisTemplate.opsForSet().add(servicesKey, layer.getServices());
            redisTemplate.opsForHash().put(layerKey, "has_services", "true");
        }
        
        // Store limits if any
        if (layer.getLimits() != null && layer.getLimits().length > 0) {
            String limitsKey = RedisKeyBuilder.layerLimits(layerId);
            redisTemplate.opsForSet().add(limitsKey, layer.getLimits());
        }
        
        // Check if layer should be active immediately (no dependencies)
        checkLayerActivation(layerId);
    }
    
    @Override
    public void updateLayerMinCores(LayerInterface layer, int minCores) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "cores_min", String.valueOf(minCores));
        
        // Update all waiting frames
        updateFrameRequirements(layer.getLayerId(), "cores_min", String.valueOf(minCores));
    }
    
    @Override
    public void updateLayerMaxCores(LayerInterface layer, int maxCores) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "cores_max", String.valueOf(maxCores));
    }
    
    @Override
    public void updateLayerMinMemory(LayerInterface layer, long minMemory) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "memory_min", String.valueOf(minMemory));
        
        // Update all waiting frames
        updateFrameRequirements(layer.getLayerId(), "memory_min", String.valueOf(minMemory));
    }
    
    @Override
    public void updateLayerMinGpu(LayerInterface layer, int minGpu) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "gpu_min", String.valueOf(minGpu));
        
        // Update all waiting frames
        updateFrameRequirements(layer.getLayerId(), "gpu_min", String.valueOf(minGpu));
    }
    
    @Override
    public void updateLayerMaxRss(LayerInterface layer, long maxRss) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "max_rss", String.valueOf(maxRss));
    }
    
    @Override
    public void updateLayerTags(LayerInterface layer, String... tags) {
        String tagsKey = RedisKeyBuilder.layerTags(layer.getLayerId());
        
        // Replace all tags
        redisTemplate.delete(tagsKey);
        if (tags != null && tags.length > 0) {
            redisTemplate.opsForSet().add(tagsKey, tags);
        }
    }
    
    @Override
    public void enableLayer(LayerInterface layer, boolean enabled) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "enabled", String.valueOf(enabled));
        
        // Update all frames
        updateFrameRequirements(layer.getLayerId(), "layer_enabled", String.valueOf(enabled));
        
        // If enabling, check if layer should be activated
        if (enabled) {
            checkLayerActivation(layer.getLayerId());
        }
    }
    
    @Override
    public void updateLayerTimeout(LayerInterface layer, int timeout) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "timeout", String.valueOf(timeout));
    }
    
    @Override
    public void updateLayerTimeoutLLU(LayerInterface layer, int timeoutLLU) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "timeout_llu", String.valueOf(timeoutLLU));
    }
    
    @Override
    public void updateLayerMemoryOptimizerEnabled(LayerInterface layer, boolean enabled) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "memory_optimizer_enabled", String.valueOf(enabled));
    }
    
    @Override
    public void updateLayerThreadable(LayerInterface layer, boolean threadable) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        redisTemplate.opsForHash().put(layerKey, "is_threadable", String.valueOf(threadable));
    }
    
    @Override
    public List<LayerInterface> getLayers(JobInterface job) {
        String jobLayersKey = RedisKeyBuilder.jobLayers(job.getJobId());
        
        // Get layers ordered by dispatch order
        Set<String> layerIds = redisTemplate.opsForZSet().range(jobLayersKey, 0, -1);
        
        if (layerIds == null || layerIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        return layerIds.stream()
            .map(this::getLayer)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    @Override
    public void updateLayerStats(LayerInterface layer, long totalFrames, long succeededFrames,
                                 long runningFrames, long waitingFrames, long dependFrames,
                                 long deadFrames, long eatenFrames) {
        String statsKey = RedisKeyBuilder.layerStats(layer.getLayerId());
        
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
        
        // Check if layer is complete
        if ((succeededFrames + eatenFrames) >= totalFrames && totalFrames > 0) {
            markLayerComplete(layer.getLayerId());
        }
    }
    
    @Override
    public boolean isLayerComplete(LayerInterface layer) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        String isComplete = (String) redisTemplate.opsForHash().get(layerKey, "is_complete");
        return "true".equals(isComplete);
    }
    
    @Override
    public void balanceLayerMinMemory(LayerInterface layer, long timestamp, long memory) {
        String layerKey = RedisKeyBuilder.layer(layer.getLayerId());
        
        // Simple memory balancing - in production this would be more sophisticated
        Object currentMinObj = redisTemplate.opsForHash().get(layerKey, "memory_min");
        if (currentMinObj != null) {
            long currentMin = Long.parseLong(currentMinObj.toString());
            if (memory > currentMin) {
                redisTemplate.opsForHash().put(layerKey, "memory_min", String.valueOf(memory));
                updateFrameRequirements(layer.getLayerId(), "memory_min", String.valueOf(memory));
            }
        }
    }
    
    // NEW: Activate layer when dependencies are satisfied
    public void activateLayer(String layerId) {
        String activateScript = 
            "local layer_key = KEYS[1]\n" +
            "local dispatch_queue = KEYS[2]\n" +
            "local frame_prefix = KEYS[3]\n" +
            "\n" +
            "-- Mark layer as active\n" +
            "redis.call('HSET', layer_key, 'active', 'true')\n" +
            "redis.call('HSET', layer_key, 'activated_time', ARGV[1])\n" +
            "\n" +
            "-- Get all frames for this layer\n" +
            "local pattern = frame_prefix .. '*:' .. ARGV[2] .. ':*'\n" +
            "local frame_keys = redis.call('KEYS', pattern)\n" +
            "\n" +
            "local activated = 0\n" +
            "for _, frame_key in ipairs(frame_keys) do\n" +
            "  local state = redis.call('HGET', frame_key, 'state')\n" +
            "  if state == 'DEPEND' then\n" +
            "    -- Check frame-specific dependencies\n" +
            "    local frame_id = string.match(frame_key, 'frame:(.+)')\n" +
            "    local deps_key = 'frame:deps:' .. frame_id\n" +
            "    local has_deps = redis.call('SCARD', deps_key)\n" +
            "    \n" +
            "    if has_deps == 0 then\n" +
            "      -- No frame dependencies, activate it\n" +
            "      redis.call('HSET', frame_key, 'state', 'WAITING')\n" +
            "      \n" +
            "      -- Add to dispatch queue\n" +
            "      local priority = tonumber(redis.call('HGET', frame_key, 'priority') or '0')\n" +
            "      local job_priority = tonumber(redis.call('HGET', frame_key, 'job_priority') or '0')\n" +
            "      local layer_order = tonumber(redis.call('HGET', frame_key, 'layer_order') or '0')\n" +
            "      local frame_number = tonumber(redis.call('HGET', frame_key, 'frame_number') or '0')\n" +
            "      \n" +
            "      local score = (job_priority * 1000000) + (priority * 10000) + ((100 - layer_order) * 100) + (10000 - frame_number)\n" +
            "      redis.call('ZADD', dispatch_queue, score, frame_id)\n" +
            "      activated = activated + 1\n" +
            "    end\n" +
            "  end\n" +
            "end\n" +
            "\n" +
            "return activated";
        
        String layerKey = RedisKeyBuilder.layer(layerId);
        Object facilityId = redisTemplate.opsForHash().get(layerKey, "facility_id");
        
        if (facilityId != null) {
            redisTemplate.execute(
                new DefaultRedisScript<>(activateScript, Long.class),
                Arrays.asList(
                    layerKey,
                    RedisKeyBuilder.dispatchQueue(facilityId.toString()),
                    RedisKeyBuilder.framePrefix()
                ),
                String.valueOf(System.currentTimeMillis()),
                layerId
            );
        }
    }
    
    // NEW: Get blocked layers
    public List<LayerInterface> getBlockedLayers() {
        String blockedLayersKey = RedisKeyBuilder.blockedLayers();
        Set<String> layerIds = redisTemplate.opsForSet().members(blockedLayersKey);
        
        if (layerIds == null || layerIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        return layerIds.stream()
            .map(this::getLayer)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    private void markLayerComplete(String layerId) {
        String layerKey = RedisKeyBuilder.layer(layerId);
        redisTemplate.opsForHash().put(layerKey, "is_complete", "true");
        redisTemplate.opsForHash().put(layerKey, "complete_time", String.valueOf(System.currentTimeMillis()));
        
        // Trigger dependency checks via DependDao
        if (dependDao instanceof DependencyDaoRedis) {
            ((DependencyDaoRedis) dependDao).onLayerComplete(layerId);
        }
    }
    
    private void checkLayerActivation(String layerId) {
        if (dependDao instanceof DependencyDaoRedis) {
            DependencyDaoRedis depDao = (DependencyDaoRedis) dependDao;
            if (depDao.checkLayerDependencies(layerId)) {
                // No dependencies or all satisfied - activate immediately
                String layerKey = RedisKeyBuilder.layer(layerId);
                redisTemplate.opsForHash().put(layerKey, "active", "true");
                redisTemplate.opsForHash().put(layerKey, "blocked_by_depends", "false");
            } else {
                // Has unsatisfied dependencies
                String blockedLayersKey = RedisKeyBuilder.blockedLayers();
                redisTemplate.opsForSet().add(blockedLayersKey, layerId);
            }
        }
    }
    
    private void updateFrameRequirements(String layerId, String field, String value) {
        // Update all frames in this layer
        String pattern = RedisKeyBuilder.framePatternByLayer(layerId);
        Set<String> frameKeys = redisTemplate.keys(pattern);
        
        if (frameKeys != null && !frameKeys.isEmpty()) {
            for (String frameKey : frameKeys) {
                Object stateObj = redisTemplate.opsForHash().get(frameKey, "state");
                if ("WAITING".equals(stateObj)) {
                    redisTemplate.opsForHash().put(frameKey, field, value);
                }
            }
        }
    }
}

package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.HostDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.host.HardwareState;
import com.imageworks.spcue.grpc.host.HostTagType;
import com.imageworks.spcue.grpc.report.HostReport;
import com.imageworks.spcue.grpc.report.RenderHost;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

@Repository
@Profile("redis")
public class HostDaoRedis implements HostDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    private static final long HOST_TIMEOUT_MILLIS = 300000; // 5 minutes
    
    @Override
    public HostEntity getHost(String id) {
        String key = RedisKeyBuilder.host(id);
        Map<Object, Object> hostData = redisTemplate.opsForHash().entries(key);
        
        if (hostData.isEmpty()) {
            throw new EntityNotFoundException("Host not found: " + id);
        }
        
        return redisDataMapper.mapToHostEntity(hostData);
    }
    
    @Override
    public HostInterface findHost(String name) {
        String indexKey = RedisKeyBuilder.hostNameIndex(name);
        String hostId = redisTemplate.opsForValue().get(indexKey);
        
        if (hostId == null) {
            return null;
        }
        
        return getHost(hostId);
    }
    
    @Override
    public void insertHost(HostReport report, AllocationInterface alloc) {
        RenderHost host = report.getHost();
        String hostId = generateHostId();
        String hostKey = RedisKeyBuilder.host(hostId);
        
        Map<String, String> hostData = new HashMap<>();
        hostData.put("id", hostId);
        hostData.put("name", host.getName());
        hostData.put("facility_id", host.getFacility());
        hostData.put("alloc_id", alloc.getAllocationId());
        hostData.put("state", host.getState().name());
        hostData.put("nimby", String.valueOf(host.getNimbyEnabled()));
        hostData.put("cores_total", String.valueOf(host.getCores()));
        hostData.put("cores_idle", String.valueOf(host.getCores()));
        hostData.put("memory_total", String.valueOf(host.getMemory()));
        hostData.put("memory_idle", String.valueOf(host.getMemory()));
        hostData.put("gpu_total", String.valueOf(host.getGpus()));
        hostData.put("gpu_idle", String.valueOf(host.getGpus()));
        hostData.put("boot_time", String.valueOf(host.getBootTime()));
        hostData.put("last_report_time", String.valueOf(System.currentTimeMillis()));
        hostData.put("os", host.getOs());
        
        redisTemplate.opsForHash().putAll(hostKey, hostData);
        
        // Create indexes
        String nameIndexKey = RedisKeyBuilder.hostNameIndex(host.getName());
        redisTemplate.opsForValue().set(nameIndexKey, hostId);
        
        // Add to active hosts sorted set
        String activeHostsKey = RedisKeyBuilder.activeHosts(host.getFacility());
        double score = calculateHostScore(host.getCores(), host.getMemory());
        redisTemplate.opsForZSet().add(activeHostsKey, hostId, score);
        
        // Set TTL for automatic cleanup
        redisTemplate.expire(hostKey, HOST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void updateHostReport(HostInterface host, HostReport report) {
        String hostKey = RedisKeyBuilder.host(host.getHostId());
        RenderHost rh = report.getHost();
        
        // Inline Lua for atomic update with state validation
        String script = 
            "local current_state = redis.call('HGET', KEYS[1], 'state') " +
            "if current_state == 'DOWN' and ARGV[1] ~= 'DOWN' then " +
            "  redis.call('HSET', KEYS[1], 'boot_time', ARGV[2]) " +
            "end " +
            "redis.call('HMSET', KEYS[1], " +
            "  'state', ARGV[1], " +
            "  'nimby', ARGV[3], " +
            "  'cores_idle', ARGV[4], " +
            "  'memory_idle', ARGV[5], " +
            "  'gpu_idle', ARGV[6], " +
            "  'load_average', ARGV[7], " +
            "  'last_report_time', ARGV[8]) " +
            "return 1";
        
        redisTemplate.execute(
            new DefaultRedisScript<>(script, Long.class),
            Arrays.asList(hostKey),
            rh.getState().name(),
            String.valueOf(System.currentTimeMillis()),
            String.valueOf(rh.getNimbyEnabled()),
            String.valueOf(rh.getCoresIdle()),
            String.valueOf(rh.getMemoryIdle()),
            String.valueOf(rh.getGpusIdle()),
            String.valueOf(rh.getLoad()),
            String.valueOf(System.currentTimeMillis())
        );
        
        // Update active hosts score
        String activeHostsKey = RedisKeyBuilder.activeHosts(host.getFacilityId());
        if (rh.getState() == HardwareState.UP && !rh.getNimbyEnabled()) {
            double score = calculateHostScore(rh.getCoresIdle(), rh.getMemoryIdle());
            redisTemplate.opsForZSet().add(activeHostsKey, host.getHostId(), score);
        } else {
            redisTemplate.opsForZSet().remove(activeHostsKey, host.getHostId());
        }
        
        // Refresh TTL
        redisTemplate.expire(hostKey, HOST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void updateHostResources(HostInterface host, int cores, long memory, int gpus) {
        String hostKey = RedisKeyBuilder.host(host.getHostId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("cores_idle", String.valueOf(cores));
        updates.put("memory_idle", String.valueOf(memory));
        updates.put("gpu_idle", String.valueOf(gpus));
        updates.put("resource_update_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(hostKey, updates);
        
        // Update score in active hosts
        String activeHostsKey = RedisKeyBuilder.activeHosts(host.getFacilityId());
        double score = calculateHostScore(cores, memory);
        redisTemplate.opsForZSet().add(activeHostsKey, host.getHostId(), score);
    }
    
    @Override
    public void updateHostState(HostInterface host, HardwareState state) {
        String hostKey = RedisKeyBuilder.host(host.getHostId());
        
        redisTemplate.opsForHash().put(hostKey, "state", state.name());
        redisTemplate.opsForHash().put(hostKey, "state_time", 
            String.valueOf(System.currentTimeMillis()));
        
        // Update active hosts membership
        String activeHostsKey = RedisKeyBuilder.activeHosts(host.getFacilityId());
        if (state == HardwareState.UP) {
            Object coresObj = redisTemplate.opsForHash().get(hostKey, "cores_idle");
            Object memoryObj = redisTemplate.opsForHash().get(hostKey, "memory_idle");
            
            if (coresObj != null && memoryObj != null) {
                int cores = Integer.parseInt(coresObj.toString());
                long memory = Long.parseLong(memoryObj.toString());
                double score = calculateHostScore(cores, memory);
                redisTemplate.opsForZSet().add(activeHostsKey, host.getHostId(), score);
            }
        } else {
            redisTemplate.opsForZSet().remove(activeHostsKey, host.getHostId());
        }
    }
    
    @Override
    public void deleteHost(HostInterface host) {
        String hostId = host.getHostId();
        String hostKey = RedisKeyBuilder.host(hostId);
        
        // Get host data for cleanup
        Map<Object, Object> hostData = redisTemplate.opsForHash().entries(hostKey);
        if (!hostData.isEmpty()) {
            String name = (String) hostData.get("name");
            String facilityId = (String) hostData.get("facility_id");
            
            // Remove from indexes
            if (name != null) {
                String nameIndexKey = RedisKeyBuilder.hostNameIndex(name);
                redisTemplate.delete(nameIndexKey);
            }
            
            if (facilityId != null) {
                String activeHostsKey = RedisKeyBuilder.activeHosts(facilityId);
                redisTemplate.opsForZSet().remove(activeHostsKey, hostId);
            }
        }
        
        // Delete host data
        redisTemplate.delete(hostKey);
        
        // Delete associated procs
        String procPattern = RedisKeyBuilder.procPattern(hostId);
        Set<String> procKeys = redisTemplate.keys(procPattern);
        if (procKeys != null && !procKeys.isEmpty()) {
            redisTemplate.delete(procKeys);
        }
    }
    
    @Override
    public List<HostInterface> getStaleHosts(int cutoffSeconds) {
        long cutoffTime = System.currentTimeMillis() - (cutoffSeconds * 1000L);
        
        // This requires scanning all hosts - consider maintaining a separate index
        String pattern = RedisKeyBuilder.hostPattern();
        Set<String> hostKeys = redisTemplate.keys(pattern);
        
        if (hostKeys == null || hostKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<HostInterface> staleHosts = new ArrayList<>();
        
        for (String hostKey : hostKeys) {
            Object lastReportObj = redisTemplate.opsForHash().get(hostKey, "last_report_time");
            if (lastReportObj != null) {
                long lastReport = Long.parseLong(lastReportObj.toString());
                if (lastReport < cutoffTime) {
                    String hostId = hostKey.substring(hostKey.lastIndexOf(":") + 1);
                    try {
                        staleHosts.add(getHost(hostId));
                    } catch (EntityNotFoundException e) {
                        // Host was deleted, skip
                    }
                }
            }
        }
        
        return staleHosts;
    }
    
    @Override
    public void addTags(HostInterface host, String... tags) {
        String tagsKey = RedisKeyBuilder.hostTags(host.getHostId());
        redisTemplate.opsForSet().add(tagsKey, tags);
    }
    
    @Override
    public void removeTags(HostInterface host, String... tags) {
        String tagsKey = RedisKeyBuilder.hostTags(host.getHostId());
        redisTemplate.opsForSet().remove(tagsKey, (Object[]) tags);
    }
    
    @Override
    public Set<String> getTags(HostInterface host) {
        String tagsKey = RedisKeyBuilder.hostTags(host.getHostId());
        Set<String> tags = redisTemplate.opsForSet().members(tagsKey);
        return tags != null ? tags : new HashSet<>();
    }
    
    @Override
    public void updateThreadMode(HostInterface host, int cores, int gpus) {
        String hostKey = RedisKeyBuilder.host(host.getHostId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("thread_mode_cores", String.valueOf(cores));
        updates.put("thread_mode_gpus", String.valueOf(gpus));
        
        redisTemplate.opsForHash().putAll(hostKey, updates);
    }
    
    private double calculateHostScore(int cores, long memory) {
        // Score based on available resources for sorting
        // Higher score = more resources available
        return cores * 1000.0 + (memory / 1_000_000.0); // memory in MB
    }
    
    private String generateHostId() {
        return UUID.randomUUID().toString();
    }
}

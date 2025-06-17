package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.ProcDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.dispatcher.Dispatcher;

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
public class ProcDaoRedis implements ProcDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    private static final long PROC_ORPHAN_CHECK_MILLIS = 300000; // 5 minutes
    
    @Override
    public VirtualProc getVirtualProc(String id) {
        String key = RedisKeyBuilder.proc(id);
        Map<Object, Object> procData = redisTemplate.opsForHash().entries(key);
        
        if (procData.isEmpty()) {
            throw new EntityNotFoundException("Proc not found: " + id);
        }
        
        return redisDataMapper.mapToVirtualProc(procData);
    }
    
    @Override
    public void insertVirtualProc(VirtualProc proc) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        
        Map<String, String> procData = new HashMap<>();
        procData.put("id", proc.getProcId());
        procData.put("host_id", proc.getHostId());
        procData.put("host_name", proc.getHostName());
        procData.put("alloc_id", proc.getAllocId());
        procData.put("frame_id", proc.getFrameId() != null ? proc.getFrameId() : "");
        procData.put("job_id", proc.getJobId() != null ? proc.getJobId() : "");
        procData.put("show_id", proc.getShowId() != null ? proc.getShowId() : "");
        procData.put("cores_reserved", String.valueOf(proc.coresReserved));
        procData.put("memory_reserved", String.valueOf(proc.memoryReserved));
        procData.put("gpu_reserved", String.valueOf(proc.gpuReserved));
        procData.put("memory_used", String.valueOf(proc.memoryUsed));
        procData.put("memory_max", String.valueOf(proc.memoryMax));
        procData.put("unbooked", String.valueOf(proc.unbooked));
        procData.put("is_local", String.valueOf(proc.isLocalDispatch));
        procData.put("created_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(procKey, procData);
        
        // Add to host's proc set
        String hostProcsKey = RedisKeyBuilder.hostProcs(proc.getHostId());
        redisTemplate.opsForSet().add(hostProcsKey, proc.getProcId());
        
        // If assigned to a frame, create booking
        if (proc.getFrameId() != null && !proc.getFrameId().isEmpty()) {
            createBooking(proc);
        }
    }
    
    @Override
    public boolean deleteVirtualProc(VirtualProc proc) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        
        // Check if proc exists
        if (!redisTemplate.hasKey(procKey)) {
            return false;
        }
        
        // Clean up booking if exists
        String frameId = proc.getFrameId();
        if (frameId != null && !frameId.isEmpty()) {
            String bookingKey = RedisKeyBuilder.booking(frameId);
            redisTemplate.delete(bookingKey);
        }
        
        // Remove from host's proc set
        String hostProcsKey = RedisKeyBuilder.hostProcs(proc.getHostId());
        redisTemplate.opsForSet().remove(hostProcsKey, proc.getProcId());
        
        // Delete proc data
        redisTemplate.delete(procKey);
        
        return true;
    }
    
    @Override
    public void updateVirtualProcAssignment(VirtualProc proc, String frameId, 
                                            String jobId, String showId) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        
        // Atomic update with booking
        String script = 
            "redis.call('HSET', KEYS[1], 'frame_id', ARGV[1]) " +
            "redis.call('HSET', KEYS[1], 'job_id', ARGV[2]) " +
            "redis.call('HSET', KEYS[1], 'show_id', ARGV[3]) " +
            "redis.call('HSET', KEYS[1], 'assigned_time', ARGV[4]) " +
            "if ARGV[1] ~= '' then " +
            "  redis.call('SETEX', KEYS[2], 86400, ARGV[5]) " + // 24 hour TTL
            "end " +
            "return 1";
        
        String bookingKey = RedisKeyBuilder.booking(frameId);
        
        redisTemplate.execute(
            new DefaultRedisScript<>(script, Long.class),
            Arrays.asList(procKey, bookingKey),
            frameId,
            jobId,
            showId,
            String.valueOf(System.currentTimeMillis()),
            proc.getProcId()
        );
    }
    
    @Override
    public void updateVirtualProcMemoryUsage(VirtualProc proc, long memoryUsed, 
                                             long memoryMax, long virtualMemoryUsed,
                                             long virtualMemoryMax) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("memory_used", String.valueOf(memoryUsed));
        updates.put("memory_max", String.valueOf(memoryMax));
        updates.put("virtual_memory_used", String.valueOf(virtualMemoryUsed));
        updates.put("virtual_memory_max", String.valueOf(virtualMemoryMax));
        updates.put("memory_update_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(procKey, updates);
    }
    
    @Override
    public void clearVirtualProcAssignment(VirtualProc proc) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        
        // Get current frame for booking cleanup
        Object frameId = redisTemplate.opsForHash().get(procKey, "frame_id");
        
        // Clear assignment
        Map<String, String> updates = new HashMap<>();
        updates.put("frame_id", "");
        updates.put("job_id", "");
        updates.put("show_id", "");
        updates.put("cleared_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(procKey, updates);
        
        // Clear booking
        if (frameId != null && !frameId.toString().isEmpty()) {
            String bookingKey = RedisKeyBuilder.booking(frameId.toString());
            redisTemplate.delete(bookingKey);
        }
    }
    
    @Override
    public void unbookVirtualProc(VirtualProc proc) {
        String procKey = RedisKeyBuilder.proc(proc.getProcId());
        redisTemplate.opsForHash().put(procKey, "unbooked", "true");
        redisTemplate.opsForHash().put(procKey, "unbook_time", 
            String.valueOf(System.currentTimeMillis()));
    }
    
    @Override
    public List<VirtualProc> findVirtualProcs(FrameInterface frame) {
        // Find procs assigned to this frame
        String frameId = frame.getFrameId();
        String pattern = RedisKeyBuilder.procPattern("*");
        
        Set<String> procKeys = redisTemplate.keys(pattern);
        if (procKeys == null || procKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<VirtualProc> procs = new ArrayList<>();
        for (String procKey : procKeys) {
            Object assignedFrameId = redisTemplate.opsForHash().get(procKey, "frame_id");
            if (frameId.equals(assignedFrameId)) {
                Map<Object, Object> procData = redisTemplate.opsForHash().entries(procKey);
                if (!procData.isEmpty()) {
                    procs.add(redisDataMapper.mapToVirtualProc(procData));
                }
            }
        }
        
        return procs;
    }
    
    @Override
    public List<VirtualProc> findVirtualProcs(HostInterface host) {
        String hostProcsKey = RedisKeyBuilder.hostProcs(host.getHostId());
        Set<String> procIds = redisTemplate.opsForSet().members(hostProcsKey);
        
        if (procIds == null || procIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        return procIds.stream()
            .map(this::getVirtualProc)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    @Override
    public List<VirtualProc> findOrphanedVirtualProcs(int secondsOrphaned) {
        long cutoffTime = System.currentTimeMillis() - (secondsOrphaned * 1000L);
        
        // Find procs that have been assigned but not updated recently
        String pattern = RedisKeyBuilder.procPattern("*");
        Set<String> procKeys = redisTemplate.keys(pattern);
        
        if (procKeys == null || procKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<VirtualProc> orphaned = new ArrayList<>();
        
        for (String procKey : procKeys) {
            Map<Object, Object> procData = redisTemplate.opsForHash().entries(procKey);
            
            String frameId = (String) procData.get("frame_id");
            String updateTimeStr = (String) procData.get("memory_update_time");
            
            if (frameId != null && !frameId.isEmpty() && updateTimeStr != null) {
                long updateTime = Long.parseLong(updateTimeStr);
                if (updateTime < cutoffTime) {
                    VirtualProc proc = redisDataMapper.mapToVirtualProc(procData);
                    orphaned.add(proc);
                }
            }
        }
        
        return orphaned;
    }
    
    @Override
    public VirtualProc findVirtualProc(FrameInterface frame, HostInterface host) {
        List<VirtualProc> hostProcs = findVirtualProcs(host);
        
        for (VirtualProc proc : hostProcs) {
            if (frame.getFrameId().equals(proc.getFrameId())) {
                return proc;
            }
        }
        
        return null;
    }
    
    @Override
    public void updateProcMemoryUsage(FrameInterface frame, long rss, long maxRss, 
                                      long vsize, long maxVsize, long usedGpuMemory,
                                      long maxGpuMemory, byte[] children) {
        List<VirtualProc> procs = findVirtualProcs(frame);
        
        if (!procs.isEmpty()) {
            // Update the first proc (usually there's only one)
            VirtualProc proc = procs.get(0);
            updateVirtualProcMemoryUsage(proc, rss, maxRss, vsize, maxVsize);
            
            // Store GPU memory if used
            if (usedGpuMemory > 0 || maxGpuMemory > 0) {
                String procKey = RedisKeyBuilder.proc(proc.getProcId());
                redisTemplate.opsForHash().put(procKey, "gpu_memory_used", 
                    String.valueOf(usedGpuMemory));
                redisTemplate.opsForHash().put(procKey, "gpu_memory_max", 
                    String.valueOf(maxGpuMemory));
            }
            
            // Store children PIDs if provided
            if (children != null && children.length > 0) {
                String procKey = RedisKeyBuilder.proc(proc.getProcId());
                redisTemplate.opsForHash().put(procKey, "children_pids", 
                    Base64.getEncoder().encodeToString(children));
            }
        }
    }
    
    private void createBooking(VirtualProc proc) {
        if (proc.getFrameId() != null && !proc.getFrameId().isEmpty()) {
            String bookingKey = RedisKeyBuilder.booking(proc.getFrameId());
            redisTemplate.opsForValue().set(bookingKey, proc.getProcId(), 
                24, TimeUnit.HOURS);
        }
    }
}

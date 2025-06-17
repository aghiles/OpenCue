package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.BookingDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.host.LockState;

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
public class BookingDaoRedis implements BookingDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    @Override
    public boolean insertLocalHostAssignment(HostInterface host, JobInterface job, 
                                             LocalHostAssignment lja) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(host.getHostId());
        
        // Check if assignment already exists
        if (redisTemplate.hasKey(ljaKey)) {
            return false;
        }
        
        Map<String, String> ljaData = new HashMap<>();
        ljaData.put("id", lja.getId());
        ljaData.put("host_id", host.getHostId());
        ljaData.put("job_id", job.getJobId());
        ljaData.put("cores", String.valueOf(lja.getIdleCoreUnits()));
        ljaData.put("memory", String.valueOf(lja.getIdleMemory()));
        ljaData.put("gpus", String.valueOf(lja.getIdleGpuUnits()));
        ljaData.put("threads", String.valueOf(lja.getThreads()));
        ljaData.put("max_cores", String.valueOf(lja.getMaxCoreUnits()));
        ljaData.put("max_memory", String.valueOf(lja.getMaxMemory()));
        ljaData.put("max_gpus", String.valueOf(lja.getMaxGpuUnits()));
        ljaData.put("created_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(ljaKey, ljaData);
        
        // Add to job's LHA set
        String jobLhaKey = RedisKeyBuilder.jobLocalHostAssignments(job.getJobId());
        redisTemplate.opsForSet().add(jobLhaKey, host.getHostId());
        
        // Add to host's LHA set
        String hostLhaKey = RedisKeyBuilder.hostLocalAssignments(host.getHostId());
        redisTemplate.opsForSet().add(hostLhaKey, job.getJobId());
        
        // Set TTL if specified
        if (lja.getDuration() > 0) {
            redisTemplate.expire(ljaKey, lja.getDuration(), TimeUnit.SECONDS);
        }
        
        return true;
    }
    
    @Override
    public boolean deleteLocalHostAssignment(LocalHostAssignment lja) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(lja.getHostId());
        
        // Get data before deletion for cleanup
        Map<Object, Object> ljaData = redisTemplate.opsForHash().entries(ljaKey);
        if (ljaData.isEmpty()) {
            return false;
        }
        
        String hostId = (String) ljaData.get("host_id");
        String jobId = (String) ljaData.get("job_id");
        
        // Remove from indexes
        if (hostId != null && jobId != null) {
            String jobLhaKey = RedisKeyBuilder.jobLocalHostAssignments(jobId);
            redisTemplate.opsForSet().remove(jobLhaKey, hostId);
            
            String hostLhaKey = RedisKeyBuilder.hostLocalAssignments(hostId);
            redisTemplate.opsForSet().remove(hostLhaKey, jobId);
        }
        
        // Delete the assignment
        redisTemplate.delete(ljaKey);
        
        return true;
    }
    
    @Override
    public void updateLocalHostAssignment(LocalHostAssignment lja, int cores, long memory, int gpus) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(lja.getHostId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("cores", String.valueOf(cores));
        updates.put("memory", String.valueOf(memory));
        updates.put("gpus", String.valueOf(gpus));
        updates.put("updated_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(ljaKey, updates);
    }
    
    @Override
    public LocalHostAssignment getLocalHostAssignment(String id) {
        // Search by ID (need to scan)
        String pattern = RedisKeyBuilder.localHostAssignmentPattern();
        Set<String> ljaKeys = redisTemplate.keys(pattern);
        
        if (ljaKeys == null || ljaKeys.isEmpty()) {
            return null;
        }
        
        for (String ljaKey : ljaKeys) {
            Object storedId = redisTemplate.opsForHash().get(ljaKey, "id");
            if (id.equals(storedId)) {
                Map<Object, Object> ljaData = redisTemplate.opsForHash().entries(ljaKey);
                return redisDataMapper.mapToLocalHostAssignment(ljaData);
            }
        }
        
        return null;
    }
    
    @Override
    public LocalHostAssignment findLocalHostAssignment(HostInterface host) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(host.getHostId());
        Map<Object, Object> ljaData = redisTemplate.opsForHash().entries(ljaKey);
        
        if (ljaData.isEmpty()) {
            return null;
        }
        
        return redisDataMapper.mapToLocalHostAssignment(ljaData);
    }
    
    @Override
    public List<LocalHostAssignment> getLocalHostAssignments(HostInterface host) {
        String hostLhaKey = RedisKeyBuilder.hostLocalAssignments(host.getHostId());
        Set<String> jobIds = redisTemplate.opsForSet().members(hostLhaKey);
        
        if (jobIds == null || jobIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        // For each job, get the LHA
        List<LocalHostAssignment> assignments = new ArrayList<>();
        for (String jobId : jobIds) {
            LocalHostAssignment lja = findLocalHostAssignment(host);
            if (lja != null) {
                assignments.add(lja);
            }
        }
        
        return assignments;
    }
    
    @Override
    public boolean hasActiveLocalHostAssignment(HostInterface host) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(host.getHostId());
        return redisTemplate.hasKey(ljaKey);
    }
    
    @Override
    public boolean incrementLhaIdleCores(LocalHostAssignment lja, int coreUnits) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(lja.getHostId());
        
        // Atomic increment with bounds checking
        String script = 
            "local current = tonumber(redis.call('HGET', KEYS[1], 'cores')) " +
            "local max = tonumber(redis.call('HGET', KEYS[1], 'max_cores')) " +
            "local new_value = current + tonumber(ARGV[1]) " +
            "if new_value <= max and new_value >= 0 then " +
            "  redis.call('HSET', KEYS[1], 'cores', new_value) " +
            "  return 1 " +
            "end " +
            "return 0";
        
        Boolean success = redisTemplate.execute(
            new DefaultRedisScript<>(script, Boolean.class),
            Arrays.asList(ljaKey),
            String.valueOf(coreUnits)
        );
        
        return Boolean.TRUE.equals(success);
    }
    
    @Override
    public void updateLocalHostAssignmentMemory(LocalHostAssignment lja, long memory) {
        String ljaKey = RedisKeyBuilder.localHostAssignment(lja.getHostId());
        redisTemplate.opsForHash().put(ljaKey, "memory", String.valueOf(memory));
    }
    
    @Override
    public List<LocalHostAssignment> getStaleLocalHostAssignments(int seconds) {
        long cutoffTime = System.currentTimeMillis() - (seconds * 1000L);
        
        String pattern = RedisKeyBuilder.localHostAssignmentPattern();
        Set<String> ljaKeys = redisTemplate.keys(pattern);
        
        if (ljaKeys == null || ljaKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<LocalHostAssignment> staleAssignments = new ArrayList<>();
        
        for (String ljaKey : ljaKeys) {
            Object updatedTimeObj = redisTemplate.opsForHash().get(ljaKey, "updated_time");
            Object createdTimeObj = redisTemplate.opsForHash().get(ljaKey, "created_time");
            
            long lastTime = 0;
            if (updatedTimeObj != null) {
                lastTime = Long.parseLong(updatedTimeObj.toString());
            } else if (createdTimeObj != null) {
                lastTime = Long.parseLong(createdTimeObj.toString());
            }
            
            if (lastTime > 0 && lastTime < cutoffTime) {
                Map<Object, Object> ljaData = redisTemplate.opsForHash().entries(ljaKey);
                LocalHostAssignment lja = redisDataMapper.mapToLocalHostAssignment(ljaData);
                staleAssignments.add(lja);
            }
        }
        
        return staleAssignments;
    }
    
    @Override
    public void insertBooking(FrameInterface frame, VirtualProc proc) {
        String bookingKey = RedisKeyBuilder.booking(frame.getFrameId());
        
        Map<String, String> bookingData = new HashMap<>();
        bookingData.put("frame_id", frame.getFrameId());
        bookingData.put("proc_id", proc.getProcId());
        bookingData.put("host_id", proc.getHostId());
        bookingData.put("created_time", String.valueOf(System.currentTimeMillis()));
        
        // Store booking with TTL
        String bookingJson = redisDataMapper.mapToJson(bookingData);
        redisTemplate.opsForValue().set(bookingKey, bookingJson, 24, TimeUnit.HOURS);
        
        // Update frame state
        String frameKey = RedisKeyBuilder.frame(frame.getFrameId());
        redisTemplate.opsForHash().put(frameKey, "proc_id", proc.getProcId());
        redisTemplate.opsForHash().put(frameKey, "host_id", proc.getHostId());
    }
    
    @Override
    public boolean deleteBooking(FrameInterface frame) {
        String bookingKey = RedisKeyBuilder.booking(frame.getFrameId());
        
        // Clear frame booking info
        String frameKey = RedisKeyBuilder.frame(frame.getFrameId());
        redisTemplate.opsForHash().delete(frameKey, "proc_id", "host_id");
        
        // Delete booking
        return redisTemplate.delete(bookingKey);
    }
    
    @Override
    public boolean hasBooking(FrameInterface frame) {
        String bookingKey = RedisKeyBuilder.booking(frame.getFrameId());
        return redisTemplate.hasKey(bookingKey);
    }
    
    @Override
    public List<String> findBookedFrames(HostInterface host) {
        // Get all procs for this host
        String hostProcsKey = RedisKeyBuilder.hostProcs(host.getHostId());
        Set<String> procIds = redisTemplate.opsForSet().members(hostProcsKey);
        
        if (procIds == null || procIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<String> frameIds = new ArrayList<>();
        
        for (String procId : procIds) {
            String procKey = RedisKeyBuilder.proc(procId);
            Object frameId = redisTemplate.opsForHash().get(procKey, "frame_id");
            if (frameId != null && !frameId.toString().isEmpty()) {
                frameIds.add(frameId.toString());
            }
        }
        
        return frameIds;
    }
    
    @Override
    public void clearStaleBookings(int staleMinutes) {
        long cutoffTime = System.currentTimeMillis() - (staleMinutes * 60 * 1000L);
        
        // This would require scanning all bookings
        // In production, consider maintaining a sorted set of booking times
        String pattern = RedisKeyBuilder.bookingPattern();
        Set<String> bookingKeys = redisTemplate.keys(pattern);
        
        if (bookingKeys == null || bookingKeys.isEmpty()) {
            return;
        }
        
        for (String bookingKey : bookingKeys) {
            String bookingJson = redisTemplate.opsForValue().get(bookingKey);
            if (bookingJson != null) {
                Map<String, Object> bookingData = redisDataMapper.jsonToMap(bookingJson);
                Long createdTime = Long.valueOf(bookingData.get("created_time").toString());
                
                if (createdTime < cutoffTime) {
                    // Extract frame ID from key
                    String frameId = bookingKey.substring(bookingKey.lastIndexOf(":") + 1);
                    
                    // Clear frame booking info
                    String frameKey = RedisKeyBuilder.frame(frameId);
                    redisTemplate.opsForHash().delete(frameKey, "proc_id", "host_id");
                    
                    // Delete stale booking
                    redisTemplate.delete(bookingKey);
                }
            }
        }
    }
}

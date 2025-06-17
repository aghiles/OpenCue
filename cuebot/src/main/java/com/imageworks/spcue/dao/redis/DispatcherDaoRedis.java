package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.DispatcherDao;
import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.dao.redis.util.RedisDataMapper;
import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.LocalHostAssignment;
import com.imageworks.spcue.grpc.host.ProcMode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.context.annotation.Profile;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Profile("redis")
public class DispatcherDaoRedis implements DispatcherDao {
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private FindDispatchableFrames findDispatchableFrames;
    
    @Autowired
    private AtomicBookingScript atomicBookingScript;
    
    @Autowired
    private RedisDataMapper redisDataMapper;
    
    @Override
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc, int limit) {
        return findDispatchableFrames.execute(
            redisTemplate,
            job.getFacilityId(),
            proc.coresReserved,
            proc.memoryReserved,
            proc.gpuReserved,
            proc.getTags(),
            limit
        );
    }
    
    @Override
    public List<DispatchFrame> findNextDispatchFrames(JobInterface job, VirtualProc proc) {
        return findNextDispatchFrames(job, proc, 100);
    }
    
    @Override
    public boolean reserveFrame(DispatchFrame frame, VirtualProc proc) {
        return atomicBookingScript.bookFrame(
            redisTemplate,
            frame.getId(),
            proc.getId(),
            proc.getHostId()
        );
    }
    
    @Override
    public DispatchHost findDispatchHost(String hostId) {
        String key = RedisKeyBuilder.host(hostId);
        Map<Object, Object> hostData = redisTemplate.opsForHash().entries(key);
        
        if (hostData.isEmpty()) {
            return null;
        }
        
        return redisDataMapper.mapToDispatchHost(hostData);
    }
    
    @Override
    public List<DispatchHost> findDispatchHosts(String facilityId, int numHosts) {
        String hostsKey = RedisKeyBuilder.activeHosts(facilityId);
        
        // Get top N hosts by available resources
        Set<String> hostIds = redisTemplate.opsForZSet()
            .reverseRange(hostsKey, 0, numHosts - 1);
        
        if (hostIds == null || hostIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        return hostIds.stream()
            .map(this::findDispatchHost)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    @Override
    public void updateFrameStarted(DispatchFrame frame, VirtualProc proc) {
        String frameKey = RedisKeyBuilder.frame(frame.getId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("state", "RUNNING");
        updates.put("proc_id", proc.getId());
        updates.put("host_name", proc.getHostName());
        updates.put("start_time", String.valueOf(System.currentTimeMillis()));
        updates.put("attempt", String.valueOf(frame.getAttempt() + 1));
        
        redisTemplate.opsForHash().putAll(frameKey, updates);
        
        // Remove from dispatch queue
        String queueKey = RedisKeyBuilder.dispatchQueue(frame.getFacilityId());
        redisTemplate.opsForZSet().remove(queueKey, frame.getId());
    }
    
    @Override
    public void updateFrameCompleted(DispatchFrame frame, int exitStatus) {
        String frameKey = RedisKeyBuilder.frame(frame.getId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("state", exitStatus == 0 ? "SUCCEEDED" : "DEPEND");
        updates.put("exit_status", String.valueOf(exitStatus));
        updates.put("stop_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(frameKey, updates);
        
        // Release booking
        String bookingKey = RedisKeyBuilder.booking(frame.getId());
        redisTemplate.delete(bookingKey);
    }
    
    @Override
    public boolean isFrameDispatched(String frameId) {
        String bookingKey = RedisKeyBuilder.booking(frameId);
        return redisTemplate.hasKey(bookingKey);
    }
    
    @Override
    public Set<String> findDispatchJobs(DispatchHost host, int numJobs) {
        String jobsKey = RedisKeyBuilder.dispatchJobs(host.getFacilityId());
        
        // Get top priority jobs
        Set<String> jobIds = redisTemplate.opsForZSet()
            .reverseRange(jobsKey, 0, numJobs - 1);
        
        return jobIds != null ? jobIds : new HashSet<>();
    }
    
    @Override
    public List<VirtualProc> findVirtualProcs(DispatchHost host) {
        String pattern = RedisKeyBuilder.procPattern(host.getId());
        Set<String> procKeys = redisTemplate.keys(pattern);
        
        if (procKeys == null || procKeys.isEmpty()) {
            return new ArrayList<>();
        }
        
        return procKeys.stream()
            .map(key -> {
                Map<Object, Object> procData = redisTemplate.opsForHash().entries(key);
                return redisDataMapper.mapToVirtualProc(procData);
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
    
    @Override
    public void updateProcAssignment(VirtualProc proc, DispatchFrame frame) {
        String procKey = RedisKeyBuilder.proc(proc.getId());
        
        Map<String, String> updates = new HashMap<>();
        updates.put("frame_id", frame.getId());
        updates.put("job_id", frame.getJobId());
        updates.put("show_id", frame.getShowId());
        updates.put("assigned_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(procKey, updates);
    }
    
    @Override
    public LocalHostAssignment findLocalHostAssignment(String hostId) {
        String key = RedisKeyBuilder.localHostAssignment(hostId);
        Map<Object, Object> data = redisTemplate.opsForHash().entries(key);
        
        if (data.isEmpty()) {
            return null;
        }
        
        return redisDataMapper.mapToLocalHostAssignment(data);
    }
}

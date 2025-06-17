package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.DispatchFrame;
import com.imageworks.spcue.DispatchHost;
import com.imageworks.spcue.JobInterface;
import com.imageworks.spcue.VirtualProc;
import com.imageworks.spcue.grpc.job.FrameState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("redis")
@Testcontainers
public class RedisDispatcherDaoTests {
    
    @Container
    public static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);
    
    @Autowired
    private DispatcherDaoRedis dispatcherDao;
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @BeforeEach
    public void setup() {
        // Clear Redis before each test
        redisTemplate.getConnectionFactory().getConnection().flushAll();
        
        // Set Redis connection properties
        System.setProperty("redis.host", redis.getHost());
        System.setProperty("redis.port", String.valueOf(redis.getFirstMappedPort()));
    }
    
    @Test
    public void testFindNextDispatchFrames() {
        // Setup test data
        String facilityId = "test-facility";
        String jobId = "test-job-123";
        String frameId1 = "test-frame-1";
        String frameId2 = "test-frame-2";
        
        // Create frames in Redis
        createTestFrame(frameId1, jobId, facilityId, 100, 1048576L, 0, 100);
        createTestFrame(frameId2, jobId, facilityId, 200, 2097152L, 0, 90);
        
        // Add frames to dispatch queue
        String queueKey = RedisKeyBuilder.dispatchQueue(facilityId);
        redisTemplate.opsForZSet().add(queueKey, frameId1, 100);
        redisTemplate.opsForZSet().add(queueKey, frameId2, 90);
        
        // Create mock job and proc
        JobInterface job = mock(JobInterface.class);
        when(job.getFacilityId()).thenReturn(facilityId);
        
        VirtualProc proc = new VirtualProc();
        proc.coresReserved = 200;
        proc.memoryReserved = 3145728L; // 3GB
        proc.gpuReserved = 0;
        
        // Test
        List<DispatchFrame> frames = dispatcherDao.findNextDispatchFrames(job, proc, 10);
        
        // Verify
        assertNotNull(frames);
        assertEquals(2, frames.size());
        assertEquals(frameId1, frames.get(0).getId());
        assertEquals(frameId2, frames.get(1).getId());
    }
    
    @Test
    public void testReserveFrame() {
        // Setup
        String frameId = "test-frame-reserve";
        String procId = "test-proc-123";
        String hostId = "test-host-123";
        
        createTestFrame(frameId, "job-123", "facility-1", 100, 1048576L, 0, 50);
        
        DispatchFrame frame = new DispatchFrame();
        frame.id = frameId;
        
        VirtualProc proc = new VirtualProc();
        proc.procId = procId;
        proc.hostId = hostId;
        
        // Test reservation
        boolean reserved = dispatcherDao.reserveFrame(frame, proc);
        
        // Verify
        assertTrue(reserved);
        
        // Check booking exists
        String bookingKey = RedisKeyBuilder.booking(frameId);
        assertTrue(redisTemplate.hasKey(bookingKey));
        
        // Try to reserve again - should fail
        boolean reservedAgain = dispatcherDao.reserveFrame(frame, proc);
        assertFalse(reservedAgain);
    }
    
    @Test
    public void testUpdateFrameStarted() {
        // Setup
        String frameId = "test-frame-start";
        String procId = "test-proc-456";
        String hostName = "test-host.local";
        
        createTestFrame(frameId, "job-456", "facility-1", 100, 1048576L, 0, 75);
        
        DispatchFrame frame = new DispatchFrame();
        frame.id = frameId;
        frame.attempt = 0;
        
        VirtualProc proc = new VirtualProc();
        proc.procId = procId;
        proc.hostName = hostName;
        
        // Test
        dispatcherDao.updateFrameStarted(frame, proc);
        
        // Verify
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
        
        assertEquals("RUNNING", frameData.get("state"));
        assertEquals(procId, frameData.get("proc_id"));
        assertEquals(hostName, frameData.get("host_name"));
        assertNotNull(frameData.get("start_time"));
        assertEquals("1", frameData.get("attempt"));
    }
    
    @Test
    public void testUpdateFrameCompleted() {
        // Setup
        String frameId = "test-frame-complete";
        createTestFrame(frameId, "job-789", "facility-1", 100, 1048576L, 0, 50);
        
        DispatchFrame frame = new DispatchFrame();
        frame.id = frameId;
        
        // Create a booking
        String bookingKey = RedisKeyBuilder.booking(frameId);
        redisTemplate.opsForValue().set(bookingKey, "proc-123");
        
        // Test successful completion
        dispatcherDao.updateFrameCompleted(frame, 0);
        
        // Verify
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
        
        assertEquals("SUCCEEDED", frameData.get("state"));
        assertEquals("0", frameData.get("exit_status"));
        assertNotNull(frameData.get("stop_time"));
        
        // Booking should be cleared
        assertFalse(redisTemplate.hasKey(bookingKey));
    }
    
    @Test
    public void testFindDispatchHosts() {
        // Setup
        String facilityId = "test-facility";
        String hostId1 = "host-1";
        String hostId2 = "host-2";
        
        createTestHost(hostId1, facilityId, 1600, 16777216L, 2); // 16 cores, 16GB, 2 GPU
        createTestHost(hostId2, facilityId, 800, 8388608L, 0);   // 8 cores, 8GB, 0 GPU
        
        // Add to active hosts
        String activeHostsKey = RedisKeyBuilder.activeHosts(facilityId);
        redisTemplate.opsForZSet().add(activeHostsKey, hostId1, 1600016.0);
        redisTemplate.opsForZSet().add(activeHostsKey, hostId2, 800008.0);
        
        // Test
        List<DispatchHost> hosts = dispatcherDao.findDispatchHosts(facilityId, 5);
        
        // Verify
        assertNotNull(hosts);
        assertEquals(2, hosts.size());
        assertEquals(hostId1, hosts.get(0).getId());
        assertEquals(hostId2, hosts.get(1).getId());
    }
    
    @Test
    public void testIsFrameDispatched() {
        // Setup
        String frameId1 = "frame-dispatched";
        String frameId2 = "frame-not-dispatched";
        
        // Create booking for frame1
        String bookingKey = RedisKeyBuilder.booking(frameId1);
        redisTemplate.opsForValue().set(bookingKey, "proc-123");
        
        // Test
        assertTrue(dispatcherDao.isFrameDispatched(frameId1));
        assertFalse(dispatcherDao.isFrameDispatched(frameId2));
    }
    
    // Helper methods
    private void createTestFrame(String frameId, String jobId, String facilityId,
                                 int cores, long memory, int gpu, int priority) {
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<String, String> frameData = new HashMap<>();
        frameData.put("frame_id", frameId);
        frameData.put("frame_name", "frame_" + frameId);
        frameData.put("job_id", jobId);
        frameData.put("job_name", "job_" + jobId);
        frameData.put("facility_id", facilityId);
        frameData.put("state", "WAITING");
        frameData.put("cores_min", String.valueOf(cores));
        frameData.put("memory_min", String.valueOf(memory));
        frameData.put("gpu_min", String.valueOf(gpu));
        frameData.put("priority", String.valueOf(priority));
        frameData.put("job_active", "true");
        frameData.put("layer_enabled", "true");
        frameData.put("show_active", "true");
        frameData.put("job_paused", "false");
        frameData.put("layer_id", "layer-123");
        frameData.put("layer_name", "render");
        frameData.put("show_id", "show-123");
        frameData.put("frame_number", "1");
        frameData.put("layer_order", "0");
        frameData.put("command", "render -frame 1");
        
        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }
    
    private void createTestHost(String hostId, String facilityId, 
                                int cores, long memory, int gpu) {
        String hostKey = RedisKeyBuilder.host(hostId);
        Map<String, String> hostData = new HashMap<>();
        hostData.put("id", hostId);
        hostData.put("name", "host-" + hostId);
        hostData.put("facility_id", facilityId);
        hostData.put("cores_idle", String.valueOf(cores));
        hostData.put("memory_idle", String.valueOf(memory));
        hostData.put("gpu_idle", String.valueOf(gpu));
        hostData.put("state", "UP");
        hostData.put("nimby", "false");
        hostData.put("os", "Linux");
        
        redisTemplate.opsForHash().putAll(hostKey, hostData);
    }
}

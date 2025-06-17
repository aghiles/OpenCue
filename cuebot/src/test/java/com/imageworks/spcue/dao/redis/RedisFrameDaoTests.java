package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.FrameDetail;
import com.imageworks.spcue.FrameInterface;
import com.imageworks.spcue.LayerInterface;
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
public class RedisFrameDaoTests {
    
    @Container
    public static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);
    
    @Autowired
    private FrameDaoRedis frameDao;
    
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
    public void testGetFrameDetail() {
        // Setup
        String frameId = "test-frame-001";
        createTestFrame(frameId);
        
        // Test
        FrameDetail frame = frameDao.getFrameDetail(frameId);
        
        // Verify
        assertNotNull(frame);
        assertEquals(frameId, frame.getId());
        assertEquals("frame_001", frame.getName());
        assertEquals("job-123", frame.getJobId());
        assertEquals("layer-456", frame.getLayerId());
        assertEquals(FrameState.WAITING, frame.getState());
        assertEquals(1, frame.getNumber());
        assertEquals(0, frame.getRetryCount());
    }
    
    @Test
    public void testUpdateFrameState() {
        // Setup
        String frameId = "test-frame-002";
        createTestFrame(frameId);
        
        FrameInterface frame = mock(FrameInterface.class);
        when(frame.getFrameId()).thenReturn(frameId);
        
        // Test state transition
        frameDao.updateFrameState(frame, FrameState.RUNNING);
        
        // Verify
        String frameKey = RedisKeyBuilder.frame(frameId);
        String state = (String) redisTemplate.opsForHash().get(frameKey, "state");
        assertEquals("RUNNING", state);
        assertNotNull(redisTemplate.opsForHash().get(frameKey, "state_time"));
        
        // Test another transition
        frameDao.updateFrameState(frame, FrameState.SUCCEEDED);
        state = (String) redisTemplate.opsForHash().get(frameKey, "state");
        assertEquals("SUCCEEDED", state);
    }
    
    @Test
    public void testUpdateFrameMemoryUsage() {
        // Setup
        String frameId = "test-frame-003";
        createTestFrame(frameId);
        
        FrameInterface frame = mock(FrameInterface.class);
        when(frame.getFrameId()).thenReturn(frameId);
        
        // Test
        long maxRss = 2097152L; // 2GB
        long rss = 1572864L;    // 1.5GB
        frameDao.updateFrameMemoryUsage(frame, maxRss, rss);
        
        // Verify
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
        
        assertEquals(String.valueOf(maxRss), frameData.get("max_rss"));
        assertEquals(String.valueOf(rss), frameData.get("used_memory"));
        assertNotNull(frameData.get("memory_time"));
    }
    
    @Test
    public void testUpdateFrameCleared() {
        // Setup
        String frameId = "test-frame-004";
        String procId = "proc-123";
        String bookingKey = RedisKeyBuilder.booking(frameId);
        
        createTestFrame(frameId);
        
        // Set frame to RUNNING state with booking
        String frameKey = RedisKeyBuilder.frame(frameId);
        redisTemplate.opsForHash().put(frameKey, "state", "RUNNING");
        redisTemplate.opsForHash().put(frameKey, "proc_id", procId);
        redisTemplate.opsForHash().put(frameKey, "host_name", "host-123");
        redisTemplate.opsForHash().put(frameKey, "start_time", "1234567890");
        redisTemplate.opsForValue().set(bookingKey, procId);
        
        FrameInterface frame = mock(FrameInterface.class);
        when(frame.getFrameId()).thenReturn(frameId);
        
        // Test
        boolean cleared = frameDao.updateFrameCleared(frame);
        
        // Verify
        assertTrue(cleared);
        
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
        assertEquals("WAITING", frameData.get("state"));
        assertNull(frameData.get("proc_id"));
        assertNull(frameData.get("host_name"));
        assertNull(frameData.get("start_time"));
        assertFalse(redisTemplate.hasKey(bookingKey));
    }
    
    @Test
    public void testUpdateFrameStopped() {
        // Setup
        String frameId = "test-frame-005";
        createTestFrame(frameId);
        
        FrameInterface frame = mock(FrameInterface.class);
        when(frame.getFrameId()).thenReturn(frameId);
        
        // Create booking
        String bookingKey = RedisKeyBuilder.booking(frameId);
        redisTemplate.opsForValue().set(bookingKey, "proc-456");
        
        // Test
        int exitStatus = 0;
        long maxRss = 3145728L;
        long usedTime = 300000L;
        frameDao.updateFrameStopped(frame, FrameState.SUCCEEDED, exitStatus, maxRss, usedTime);
        
        // Verify
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<Object, Object> frameData = redisTemplate.opsForHash().entries(frameKey);
        
        assertEquals("SUCCEEDED", frameData.get("state"));
        assertEquals("0", frameData.get("exit_status"));
        assertEquals(String.valueOf(maxRss), frameData.get("max_rss"));
        assertEquals(String.valueOf(usedTime), frameData.get("used_time"));
        assertNotNull(frameData.get("stop_time"));
        
        // Booking should be cleared
        assertFalse(redisTemplate.hasKey(bookingKey));
    }
    
    @Test
    public void testMarkFramesWaiting() {
        // Setup
        String layerId = "layer-789";
        String showId = "show-123";
        String facilityId = "facility-1";
        
        // Create multiple frames for the layer
        List<String> frameIds = Arrays.asList("frame-1", "frame-2", "frame-3");
        for (String frameId : frameIds) {
            createTestFrame(frameId, layerId);
            // Set some to DEAD state
            if (frameId.equals("frame-2")) {
                String frameKey = RedisKeyBuilder.frame(frameId);
                redisTemplate.opsForHash().put(frameKey, "state", "DEAD");
            }
        }
        
        LayerInterface layer = mock(LayerInterface.class);
        when(layer.getLayerId()).thenReturn(layerId);
        when(layer.getShowId()).thenReturn(showId);
        when(layer.getFacilityId()).thenReturn(facilityId);
        
        // Test
        frameDao.markFramesWaiting(layer);
        
        // Verify - all frames should be WAITING
        for (String frameId : frameIds) {
            String frameKey = RedisKeyBuilder.frame(frameId);
            String state = (String) redisTemplate.opsForHash().get(frameKey, "state");
            assertEquals("WAITING", state);
        }
    }
    
    @Test
    public void testRetryFrames() {
        // Setup
        String jobId = "job-retry-test";
        String facilityId = "facility-1";
        
        // Create frames with different states and retry counts
        createTestFrameWithState("frame-retry-1", jobId, "DEAD", 0);
        createTestFrameWithState("frame-retry-2", jobId, "DEAD", 1);
        createTestFrameWithState("frame-retry-3", jobId, "SUCCEEDED", 0);
        createTestFrameWithState("frame-retry-4", jobId, "DEAD", 3); // Exceeds max retries
        
        JobInterface job = mock(JobInterface.class);
        when(job.getJobId()).thenReturn(jobId);
        
        // Test with max retries = 2
        int retried = frameDao.retryFrames(job, FrameState.DEAD, 2);
        
        // Verify
        assertEquals(2, retried); // Only frame-retry-1 and frame-retry-2 should be retried
        
        // Check frame states
        String frame1Key = RedisKeyBuilder.frame("frame-retry-1");
        String frame2Key = RedisKeyBuilder.frame("frame-retry-2");
        String frame3Key = RedisKeyBuilder.frame("frame-retry-3");
        String frame4Key = RedisKeyBuilder.frame("frame-retry-4");
        
        assertEquals("WAITING", redisTemplate.opsForHash().get(frame1Key, "state"));
        assertEquals("WAITING", redisTemplate.opsForHash().get(frame2Key, "state"));
        assertEquals("SUCCEEDED", redisTemplate.opsForHash().get(frame3Key, "state"));
        assertEquals("DEAD", redisTemplate.opsForHash().get(frame4Key, "state"));
    }
    
    // Helper methods
    private void createTestFrame(String frameId) {
        createTestFrame(frameId, "layer-456");
    }
    
    private void createTestFrame(String frameId, String layerId) {
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<String, String> frameData = new HashMap<>();
        frameData.put("id", frameId);
        frameData.put("frame_name", "frame_001");
        frameData.put("job_id", "job-123");
        frameData.put("job_name", "test_job");
        frameData.put("layer_id", layerId);
        frameData.put("layer_name", "render");
        frameData.put("show_id", "show-456");
        frameData.put("facility_id", "facility-1");
        frameData.put("state", "WAITING");
        frameData.put("frame_number", "1");
        frameData.put("layer_order", "0");
        frameData.put("retry_count", "0");
        frameData.put("cores_min", "100");
        frameData.put("memory_min", "1048576");
        frameData.put("gpu_min", "0");
        frameData.put("priority", "50");
        frameData.put("job_active", "true");
        frameData.put("layer_enabled", "true");
        frameData.put("show_active", "true");
        frameData.put("job_paused", "false");
        
        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }
    
    private void createTestFrameWithState(String frameId, String jobId, 
                                          String state, int retryCount) {
        String frameKey = RedisKeyBuilder.frame(frameId);
        Map<String, String> frameData = new HashMap<>();
        frameData.put("id", frameId);
        frameData.put("frame_name", frameId);
        frameData.put("job_id", jobId);
        frameData.put("layer_id", "layer-123");
        frameData.put("facility_id", "facility-1");
        frameData.put("state", state);
        frameData.put("retry_count", String.valueOf(retryCount));
        frameData.put("priority", "50");
        frameData.put("frame_number", "1");
        frameData.put("layer_order", "0");
        
        redisTemplate.opsForHash().putAll(frameKey, frameData);
    }
}

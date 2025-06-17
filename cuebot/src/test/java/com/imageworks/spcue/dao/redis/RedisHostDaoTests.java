package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.dao.redis.util.RedisKeyBuilder;
import com.imageworks.spcue.HostEntity;
import com.imageworks.spcue.HostInterface;
import com.imageworks.spcue.AllocationInterface;
import com.imageworks.spcue.grpc.host.HardwareState;
import com.imageworks.spcue.grpc.report.HostReport;
import com.imageworks.spcue.grpc.report.RenderHost;

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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("redis")
@Testcontainers
public class RedisHostDaoTests {
    
    @Container
    public static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);
    
    @Autowired
    private HostDaoRedis hostDao;
    
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
    public void testInsertHost() {
        // Setup
        String hostName = "test-host-001.local";
        String facilityId = "facility-1";
        String allocId = "alloc-123";
        
        RenderHost renderHost = RenderHost.newBuilder()
            .setName(hostName)
            .setFacility(facilityId)
            .setState(HardwareState.UP)
            .setNimbyEnabled(false)
            .setCores(16)
            .setMemory(16777216) // 16GB in KB
            .setGpus(2)
            .setBootTime(System.currentTimeMillis() / 1000)
            .setOs("Linux")
            .build();
        
        HostReport report = HostReport.newBuilder()
            .setHost(renderHost)
            .build();
        
        AllocationInterface alloc = mock(AllocationInterface.class);
        when(alloc.getAllocationId()).thenReturn(allocId);
        
        // Test
        hostDao.insertHost(report, alloc);
        
        // Verify - host should be findable by name
        HostInterface host = hostDao.findHost(hostName);
        assertNotNull(host);
        assertEquals(hostName, host.getName());
        
        // Verify host data
        String hostKey = RedisKeyBuilder.host(host.getHostId());
        Map<Object, Object> hostData = redisTemplate.opsForHash().entries(hostKey);
        
        assertEquals(hostName, hostData.get("name"));
        assertEquals(facilityId, hostData.get("facility_id"));
        assertEquals(allocId, hostData.get("alloc_id"));
        assertEquals("UP", hostData.get("state"));
        assertEquals("false", hostData.get("nimby"));
        assertEquals("16", hostData.get("cores_total"));
        assertEquals("16", hostData.get("cores_idle"));
        assertEquals("16777216", hostData.get("memory_total"));
        assertEquals("16777216", hostData.get("memory_idle"));
        assertEquals("2", hostData.get("gpu_total"));
        assertEquals("2", hostData.get("gpu_idle"));
        
        // Verify indexes
        String nameIndexKey = RedisKeyBuilder.hostNameIndex(hostName);
        assertTrue(redisTemplate.hasKey(nameIndexKey));
        
        // Verify in active hosts set
        String activeHostsKey = RedisKeyBuilder.activeHosts(facilityId);
        assertTrue(redisTemplate.opsForZSet().rank(activeHostsKey, host.getHostId()) != null);
    }
    
    @Test
    public void testUpdateHostReport() {
        // Setup - create a host first
        String hostId = "host-123";
        String hostName = "test-host-002.local";
        createTestHost(hostId, hostName, "facility-1");
        
        HostInterface host = mock(HostInterface.class);
        when(host.getHostId()).thenReturn(hostId);
        when(host.getFacilityId()).thenReturn("facility-1");
        
        // Create updated report
        RenderHost updatedHost = RenderHost.newBuilder()
            .setName(hostName)
            .setState(HardwareState.UP)
            .setNimbyEnabled(true)
            .setCoresIdle(8)
            .setMemoryIdle(8388608) // 8GB
            .setGpusIdle(1)
            .setLoad(2.5f)
            .build();
        
        HostReport report = HostReport.newBuilder()
            .setHost(updatedHost)
            .build();
        
        // Test
        hostDao.updateHostReport(host, report);
        
        // Verify updates
        String hostKey = RedisKeyBuilder.host(hostId);
        Map<Object, Object> hostData = redisTemplate.opsForHash().entries(hostKey);
        
        assertEquals("UP", hostData.get("state"));
        assertEquals("true", hostData.get("nimby"));
        assertEquals("8", hostData.get("cores_idle"));
        assertEquals("8388608", hostData.get("memory_idle"));
        assertEquals("1", hostData.get("gpu_idle"));
        assertEquals("2.5", hostData.get("load_average"));
        assertNotNull(hostData.get("last_report_time"));
        
        // Should be removed from active hosts due to nimby
        String activeHostsKey = RedisKeyBuilder.activeHosts("facility-1");
        assertNull(redisTemplate.opsForZSet().rank(activeHostsKey, hostId));
    }
    
    @Test
    public void testUpdateHostState() {
        // Setup
        String hostId = "host-456";
        createTestHost(hostId, "test-host-003.local", "facility-1");
        
        HostInterface host = mock(HostInterface.class);
        when(host.getHostId()).thenReturn(hostId);
        when(host.getFacilityId()).thenReturn("facility-1");
        
        // Test state change to DOWN
        hostDao.updateHostState(host, HardwareState.DOWN);
        
        // Verify
        String hostKey = RedisKeyBuilder.host(hostId);
        assertEquals("DOWN", redisTemplate.opsForHash().get(hostKey, "state"));
        assertNotNull(redisTemplate.opsForHash().get(hostKey, "state_time"));
        
        // Should be removed from active hosts
        String activeHostsKey = RedisKeyBuilder.activeHosts("facility-1");
        assertNull(redisTemplate.opsForZSet().rank(activeHostsKey, hostId));
        
        // Test state change back to UP
        hostDao.updateHostState(host, HardwareState.UP);
        
        assertEquals("UP", redisTemplate.opsForHash().get(hostKey, "state"));
        assertNotNull(redisTemplate.opsForZSet().rank(activeHostsKey, hostId));
    }
    
    @Test
    public void testDeleteHost() {
        // Setup
        String hostId = "host-789";
        String hostName = "test-host-004.local";
        String facilityId = "facility-1";
        createTestHost(hostId, hostName, facilityId);
        
        // Create some procs for the host
        String procId1 = "proc-1";
        String procId2 = "proc-2";
        createTestProc(procId1, hostId);
        createTestProc(procId2, hostId);
        
        // Add to active hosts
        String activeHostsKey = RedisKeyBuilder.activeHosts(facilityId);
        redisTemplate.opsForZSet().add(activeHostsKey, hostId, 100.0);
        
        HostInterface host = mock(HostInterface.class);
        when(host.getHostId()).thenReturn(hostId);
        
        // Test
        hostDao.deleteHost(host);
        
        // Verify
        String hostKey = RedisKeyBuilder.host(hostId);
        assertFalse(redisTemplate.hasKey(hostKey));
        
        // Name index should be deleted
        String nameIndexKey = RedisKeyBuilder.hostNameIndex(hostName);
        assertFalse(redisTemplate.hasKey(nameIndexKey));
        
        // Should be removed from active hosts
        assertNull(redisTemplate.opsForZSet().rank(activeHostsKey, hostId));
        
        // Procs should be deleted
        assertFalse(redisTemplate.hasKey(RedisKeyBuilder.proc(procId1)));
        assertFalse(redisTemplate.hasKey(RedisKeyBuilder.proc(procId2)));
    }
    
    @Test
    public void testHostTags() {
        // Setup
        String hostId = "host-tags-test";
        createTestHost(hostId, "test-host-tags.local", "facility-1");
        
        HostInterface host = mock(HostInterface.class);
        when(host.getHostId()).thenReturn(hostId);
        
        // Test adding tags
        hostDao.addTags(host, "gpu", "linux", "high-memory");
        
        // Verify
        Set<String> tags = hostDao.getTags(host);
        assertEquals(3, tags.size());
        assertTrue(tags.contains("gpu"));
        assertTrue(tags.contains("linux"));
        assertTrue(tags.contains("high-memory"));
        
        // Test removing tags
        hostDao.removeTags(host, "gpu");
        
        tags = hostDao.getTags(host);
        assertEquals(2, tags.size());
        assertFalse(tags.contains("gpu"));
        assertTrue(tags.contains("linux"));
        assertTrue(tags.contains("high-memory"));
    }
    
    @Test
    public void testGetStaleHosts() throws InterruptedException {
        // Setup - create hosts with different report times
        long now = System.currentTimeMillis();
        
        String hostId1 = "stale-host-1";
        createTestHostWithReportTime(hostId1, "stale-1.local", now - 400000); // 6.6 minutes ago
        
        String hostId2 = "fresh-host-1";
        createTestHostWithReportTime(hostId2, "fresh-1.local", now - 60000); // 1 minute ago
        
        String hostId3 = "stale-host-2";
        createTestHostWithReportTime(hostId3, "stale-2.local", now - 320000); // 5.3 minutes ago
        
        // Test - find hosts not reported in last 5 minutes
        List<HostInterface> staleHosts = hostDao.getStaleHosts(300);
        
        // Verify
        assertEquals(2, staleHosts.size());
        
        Set<String> staleHostIds = new HashSet<>();
        for (HostInterface host : staleHosts) {
            staleHostIds.add(host.getHostId());
        }
        
        assertTrue(staleHostIds.contains(hostId1));
        assertTrue(staleHostIds.contains(hostId3));
        assertFalse(staleHostIds.contains(hostId2));
    }
    
    // Helper methods
    private void createTestHost(String hostId, String hostName, String facilityId) {
        String hostKey = RedisKeyBuilder.host(hostId);
        Map<String, String> hostData = new HashMap<>();
        hostData.put("id", hostId);
        hostData.put("name", hostName);
        hostData.put("facility_id", facilityId);
        hostData.put("alloc_id", "alloc-default");
        hostData.put("state", "UP");
        hostData.put("nimby", "false");
        hostData.put("cores_total", "16");
        hostData.put("cores_idle", "16");
        hostData.put("memory_total", "16777216");
        hostData.put("memory_idle", "16777216");
        hostData.put("gpu_total", "2");
        hostData.put("gpu_idle", "2");
        hostData.put("os", "Linux");
        hostData.put("last_report_time", String.valueOf(System.currentTimeMillis()));
        
        redisTemplate.opsForHash().putAll(hostKey, hostData);
        
        // Create name index
        String nameIndexKey = RedisKeyBuilder.hostNameIndex(hostName);
        redisTemplate.opsForValue().set(nameIndexKey, hostId);
    }
    
    private void createTestHostWithReportTime(String hostId, String hostName, long reportTime) {
        String hostKey = RedisKeyBuilder.host(hostId);
        Map<String, String> hostData = new HashMap<>();
        hostData.put("id", hostId);
        hostData.put("name", hostName);
        hostData.put("facility_id", "facility-1");
        hostData.put("state", "UP");
        hostData.put("last_report_time", String.valueOf(reportTime));
        
        redisTemplate.opsForHash().putAll(hostKey, hostData);
    }
    
    private void createTestProc(String procId, String hostId) {
        String procKey = RedisKeyBuilder.proc(procId);
        Map<String, String> procData = new HashMap<>();
        procData.put("id", procId);
        procData.put("host_id", hostId);
        
        redisTemplate.opsForHash().putAll(procKey, procData);
        
        // Add to host's proc set
        String hostProcsKey = RedisKeyBuilder.hostProcs(hostId);
        redisTemplate.opsForSet().add(hostProcsKey, procId);
    }
}

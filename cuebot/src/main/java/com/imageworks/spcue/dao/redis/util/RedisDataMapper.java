package com.imageworks.spcue.dao.redis.util;

import com.imageworks.spcue.*;
import com.imageworks.spcue.grpc.host.HardwareState;
import com.imageworks.spcue.grpc.host.LockState;
import com.imageworks.spcue.grpc.job.FrameState;
import com.imageworks.spcue.grpc.job.JobState;
import com.imageworks.spcue.grpc.job.LayerType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Utility class for mapping between Redis data structures and domain objects
 */
@Component
public class RedisDataMapper {
    
    private final ObjectMapper objectMapper;
    
    public RedisDataMapper() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    // JSON conversion methods
    public String mapToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }
    
    public Map<String, Object> jsonToMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON to map", e);
        }
    }
    
    public DispatchFrame jsonToDispatchFrame(String json) {
        try {
            Map<String, Object> data = objectMapper.readValue(json, 
                new TypeReference<Map<String, Object>>() {});
            return mapToDispatchFrame(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize DispatchFrame", e);
        }
    }
    
    // Frame mappings
    public DispatchFrame mapToDispatchFrame(Map<Object, Object> data) {
        DispatchFrame frame = new DispatchFrame();
        
        frame.id = getString(data, "frame_id");
        frame.name = getString(data, "frame_name");
        frame.layerId = getString(data, "layer_id");
        frame.jobId = getString(data, "job_id");
        frame.showId = getString(data, "show_id");
        frame.facilityId = getString(data, "facility_id");
        
        frame.state = FrameState.valueOf(getString(data, "state", "WAITING"));
        frame.command = getString(data, "command");
        frame.services = getString(data, "services");
        
        frame.minCores = getInt(data, "cores_min", 100);
        frame.minMemory = getLong(data, "memory_min", 1048576); // 1GB default
        frame.minGpu = getInt(data, "gpu_min", 0);
        
        frame.number = getInt(data, "frame_number", 1);
        frame.layerOrder = getInt(data, "layer_order", 0);
        frame.priority = getInt(data, "priority", 0);
        frame.retries = getInt(data, "retry_count", 0);
        
        return frame;
    }
    
    public FrameDetail mapToFrameDetail(Map<Object, Object> data) {
        FrameDetail frame = new FrameDetail();
        
        frame.id = getString(data, "id");
        frame.name = getString(data, "frame_name");
        frame.layerId = getString(data, "layer_id");
        frame.layerName = getString(data, "layer_name");
        frame.jobId = getString(data, "job_id");
        frame.jobName = getString(data, "job_name");
        frame.showId = getString(data, "show_id");
        
        frame.state = FrameState.valueOf(getString(data, "state", "WAITING"));
        frame.number = getInt(data, "frame_number", 1);
        frame.layerOrder = getInt(data, "layer_order", 0);
        frame.retryCount = getInt(data, "retry_count", 0);
        frame.exitStatus = getInt(data, "exit_status", -1);
        
        frame.maxRss = getLong(data, "max_rss", 0);
        frame.usedMemory = getLong(data, "used_memory", 0);
        
        frame.startTime = getLong(data, "start_time", 0);
        frame.stopTime = getLong(data, "stop_time", 0);
        
        frame.lastResource = getString(data, "host_name", "");
        
        return frame;
    }
    
    // Job mappings
    public JobDetail mapToJobDetail(Map<Object, Object> data) {
        JobDetail job = new JobDetail();
        
        job.id = getString(data, "id");
        job.name = getString(data, "name");
        job.showId = getString(data, "show_id");
        job.facilityId = getString(data, "facility_id");
        job.deptId = getString(data, "dept_id");
        
        job.state = JobState.valueOf(getString(data, "state", "PENDING"));
        job.priority = getInt(data, "priority", 0);
        job.minCores = getInt(data, "min_cores", 100);
        job.maxCores = getInt(data, "max_cores", 20000);
        job.minMemory = getLong(data, "min_memory", 1048576);
        job.minGpu = getInt(data, "min_gpu", 0);
        
        job.shot = getString(data, "shot", "");
        job.email = getString(data, "email", "");
        job.user = getString(data, "user", "");
        job.uid = getInt(data, "uid", 0);
        job.logDir = getString(data, "log_dir", "");
        
        job.isPaused = getBoolean(data, "paused", false);
        job.isAutoBook = getBoolean(data, "auto_book", false);
        job.isLocal = getBoolean(data, "is_local", false);
        
        job.startTime = getLong(data, "start_time", System.currentTimeMillis());
        job.maxRetries = getInt(data, "max_retries", 2);
        
        return job;
    }
    
    // Layer mappings
    public LayerDetail mapToLayerDetail(Map<Object, Object> data) {
        LayerDetail layer = new LayerDetail();
        
        layer.id = getString(data, "id");
        layer.jobId = getString(data, "job_id");
        layer.name = getString(data, "name");
        
        layer.type = LayerType.valueOf(getString(data, "type", "RENDER"));
        layer.command = getString(data, "command", "");
        layer.range = getString(data, "range", "");
        
        layer.chunkSize = getInt(data, "chunk_size", 1);
        layer.dispatchOrder = getInt(data, "dispatch_order", 0);
        
        layer.minCores = getInt(data, "cores_min", 100);
        layer.maxCores = getInt(data, "cores_max", 0);
        layer.isThreadable = getBoolean(data, "is_threadable", false);
        layer.minMemory = getLong(data, "memory_min", 1048576);
        layer.minGpu = getInt(data, "gpu_min", 0);
        
        layer.timeout = getInt(data, "timeout", 0);
        layer.timeoutLLU = getInt(data, "timeout_llu", 0);
        
        return layer;
    }
    
    // Host mappings
    public HostEntity mapToHostEntity(Map<Object, Object> data) {
        HostEntity host = new HostEntity();
        
        host.id = getString(data, "id");
        host.name = getString(data, "name");
        host.allocId = getString(data, "alloc_id");
        host.facilityId = getString(data, "facility_id");
        
        host.state = HardwareState.valueOf(getString(data, "state", "UP"));
        host.lockState = LockState.valueOf(getString(data, "lock_state", "OPEN"));
        
        host.isNimby = getBoolean(data, "nimby", false);
        host.dateCreated = getLong(data, "created_time", System.currentTimeMillis());
        host.dateBooted = getLong(data, "boot_time", System.currentTimeMillis());
        host.datePinged = getLong(data, "last_report_time", System.currentTimeMillis());
        
        host.cores = getInt(data, "cores_total", 0);
        host.coresIdle = getInt(data, "cores_idle", 0);
        host.memory = getLong(data, "memory_total", 0);
        host.memoryIdle = getLong(data, "memory_idle", 0);
        host.gpus = getInt(data, "gpu_total", 0);
        host.gpusIdle = getInt(data, "gpu_idle", 0);
        
        host.load = getInt(data, "load_average", 0);
        host.os = getString(data, "os", "Linux");
        
        return host;
    }
    
    public DispatchHost mapToDispatchHost(Map<Object, Object> data) {
        DispatchHost host = new DispatchHost();
        
        host.id = getString(data, "id");
        host.name = getString(data, "name");
        host.facilityId = getString(data, "facility_id");
        host.allocId = getString(data, "alloc_id");
        
        host.isNimby = getBoolean(data, "nimby", false);
        host.idleCores = getInt(data, "cores_idle", 0);
        host.idleMemory = getLong(data, "memory_idle", 0);
        host.idleGpus = getInt(data, "gpu_idle", 0);
        
        host.os = getString(data, "os", "Linux");
        
        return host;
    }
    
    // Proc mappings
    public VirtualProc mapToVirtualProc(Map<Object, Object> data) {
        VirtualProc proc = VirtualProc.build(
            getString(data, "host_id"),
            getString(data, "frame_id", "")
        );
        
        proc.procId = getString(data, "id");
        proc.hostName = getString(data, "host_name");
        proc.allocId = getString(data, "alloc_id");
        proc.jobId = getString(data, "job_id", "");
        proc.showId = getString(data, "show_id", "");
        
        proc.coresReserved = getInt(data, "cores_reserved", 0);
        proc.memoryReserved = getLong(data, "memory_reserved", 0);
        proc.gpuReserved = getInt(data, "gpu_reserved", 0);
        
        proc.memoryUsed = getLong(data, "memory_used", 0);
        proc.memoryMax = getLong(data, "memory_max", 0);
        
        proc.unbooked = getBoolean(data, "unbooked", false);
        proc.isLocalDispatch = getBoolean(data, "is_local", false);
        
        return proc;
    }
    
    // Local host assignment mappings
    public LocalHostAssignment mapToLocalHostAssignment(Map<Object, Object> data) {
        LocalHostAssignment lha = new LocalHostAssignment();
        
        lha.id = getString(data, "id");
        lha.hostId = getString(data, "host_id");
        lha.jobId = getString(data, "job_id");
        
        lha.idleCoreUnits = getInt(data, "cores", 0);
        lha.idleMemory = getLong(data, "memory", 0);
        lha.idleGpuUnits = getInt(data, "gpus", 0);
        
        lha.maxCoreUnits = getInt(data, "max_cores", 0);
        lha.maxMemory = getLong(data, "max_memory", 0);
        lha.maxGpuUnits = getInt(data, "max_gpus", 0);
        
        lha.threads = getInt(data, "threads", 1);
        
        return lha;
    }
    
    // Helper methods
    private String getString(Map<Object, Object> data, String key) {
        return getString(data, key, "");
    }
    
    private String getString(Map<Object, Object> data, String key, String defaultValue) {
        Object value = data.get(key);
        return value != null ? value.toString() : defaultValue;
    }
    
    private int getInt(Map<Object, Object> data, String key, int defaultValue) {
        Object value = data.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    private long getLong(Map<Object, Object> data, String key, long defaultValue) {
        Object value = data.get(key);
        if (value != null) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    private boolean getBoolean(Map<Object, Object> data, String key, boolean defaultValue) {
        Object value = data.get(key);
        if (value != null) {
            return "true".equalsIgnoreCase(value.toString());
        }
        return defaultValue;
    }
}

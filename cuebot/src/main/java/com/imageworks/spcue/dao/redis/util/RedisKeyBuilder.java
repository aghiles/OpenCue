package com.imageworks.spcue.dao.redis.util;

import org.springframework.stereotype.Component;

/**
 * Utility class for building consistent Redis keys across the application
 */
@Component
public class RedisKeyBuilder {
    
    private static final String SEPARATOR = ":";
    
    // Frame keys
    public static String frame(String frameId) {
        return "frame" + SEPARATOR + frameId;
    }
    
    public static String framePrefix() {
        return "frame" + SEPARATOR;
    }
    
    public static String framePattern(String jobId) {
        return "frame" + SEPARATOR + jobId + SEPARATOR + "*";
    }
    
    public static String framePatternByLayer(String layerId) {
        return "frame" + SEPARATOR + "*" + SEPARATOR + layerId + SEPARATOR + "*";
    }
    
    // Job keys
    public static String job(String jobId) {
        return "job" + SEPARATOR + jobId;
    }
    
    public static String jobPrefix() {
        return "job" + SEPARATOR;
    }
    
    public static String jobPattern() {
        return "job" + SEPARATOR + "*";
    }
    
    public static String jobNameIndex(String jobName) {
        return "idx" + SEPARATOR + "job" + SEPARATOR + "name" + SEPARATOR + jobName;
    }
    
    public static String jobStats(String jobId) {
        return "stats" + SEPARATOR + "job" + SEPARATOR + jobId;
    }
    
    public static String jobLayers(String jobId) {
        return "job" + SEPARATOR + jobId + SEPARATOR + "layers";
    }
    
    public static String jobLocalHostAssignments(String jobId) {
        return "job" + SEPARATOR + jobId + SEPARATOR + "lha";
    }
    
    public static String jobDependencies(String jobId) {
        return "job" + SEPARATOR + "deps" + SEPARATOR + jobId;
    }
    
    public static String jobDependents(String jobId) {
        return "job" + SEPARATOR + "dependents" + SEPARATOR + jobId;
    }
    
    // Layer keys
    public static String layer(String layerId) {
        return "layer" + SEPARATOR + layerId;
    }
    
    public static String layerPrefix() {
        return "layer" + SEPARATOR;
    }
    
    public static String layerPattern(String jobId) {
        return "layer" + SEPARATOR + jobId + SEPARATOR + "*";
    }
    
    public static String layerNameIndex(String jobId, String layerName) {
        return "idx" + SEPARATOR + "layer" + SEPARATOR + jobId + SEPARATOR + layerName;
    }
    
    public static String layerStats(String layerId) {
        return "stats" + SEPARATOR + "layer" + SEPARATOR + layerId;
    }
    
    public static String layerServices(String layerId) {
        return "layer" + SEPARATOR + layerId + SEPARATOR + "services";
    }
    
    public static String layerServicePrefix() {
        return "layer" + SEPARATOR + "services" + SEPARATOR;
    }
    
    public static String layerLimits(String layerId) {
        return "layer" + SEPARATOR + layerId + SEPARATOR + "limits";
    }
    
    public static String layerTags(String layerId) {
        return "layer" + SEPARATOR + layerId + SEPARATOR + "tags";
    }
    
    public static String layerDependencies(String layerId) {
        return "layer" + SEPARATOR + "deps" + SEPARATOR + layerId;
    }
    
    public static String layerDependents(String layerId) {
        return "layer" + SEPARATOR + "dependents" + SEPARATOR + layerId;
    }
    
    public static String layerActive(String layerId) {
        return "layer" + SEPARATOR + "active" + SEPARATOR + layerId;
    }
    
    // Host keys
    public static String host(String hostId) {
        return "host" + SEPARATOR + hostId;
    }
    
    public static String hostPattern() {
        return "host" + SEPARATOR + "*";
    }
    
    public static String hostNameIndex(String hostName) {
        return "idx" + SEPARATOR + "host" + SEPARATOR + "name" + SEPARATOR + hostName;
    }
    
    public static String hostProcs(String hostId) {
        return "host" + SEPARATOR + hostId + SEPARATOR + "procs";
    }
    
    public static String hostTags(String hostId) {
        return "host" + SEPARATOR + hostId + SEPARATOR + "tags";
    }
    
    public static String hostLocalAssignments(String hostId) {
        return "host" + SEPARATOR + hostId + SEPARATOR + "local";
    }
    
    // Proc keys
    public static String proc(String procId) {
        return "proc" + SEPARATOR + procId;
    }
    
    public static String procPattern(String hostId) {
        return "proc" + SEPARATOR + "*" + SEPARATOR + hostId + SEPARATOR + "*";
    }
    
    // Show keys
    public static String show(String showId) {
        return "show" + SEPARATOR + showId;
    }
    
    public static String showJobs(String showId) {
        return "show" + SEPARATOR + showId + SEPARATOR + "jobs";
    }
    
    // Booking keys
    public static String booking(String frameId) {
        return "booking" + SEPARATOR + frameId;
    }
    
    public static String bookingPrefix() {
        return "booking" + SEPARATOR;
    }
    
    public static String bookingPattern() {
        return "booking" + SEPARATOR + "*";
    }
    
    // Local host assignment keys
    public static String localHostAssignment(String hostId) {
        return "lha" + SEPARATOR + hostId;
    }
    
    public static String localHostAssignmentPattern() {
        return "lha" + SEPARATOR + "*";
    }
    
    // Dispatch queue keys
    public static String dispatchQueue(String facilityId) {
        return "dispatch" + SEPARATOR + "queue" + SEPARATOR + facilityId;
    }
    
    public static String dispatchQueuePrefix() {
        return "dispatch" + SEPARATOR + "queue" + SEPARATOR;
    }
    
    public static String dispatchJobs(String facilityId) {
        return "dispatch" + SEPARATOR + "jobs" + SEPARATOR + facilityId;
    }
    
    public static String dispatchJobsPrefix() {
        return "dispatch" + SEPARATOR + "jobs" + SEPARATOR;
    }
    
    public static String blockedLayers() {
        return "dispatch" + SEPARATOR + "blocked" + SEPARATOR + "layers";
    }
    
    // Active hosts sorted set
    public static String activeHosts() {
        return "active" + SEPARATOR + "hosts";
    }
    
    public static String activeHosts(String facilityId) {
        return "active" + SEPARATOR + "hosts" + SEPARATOR + facilityId;
    }
    
    // Stale frames queue
    public static String staleFramesQueue() {
        return "stale" + SEPARATOR + "frames";
    }
    
    // Statistics keys
    public static String statsDispatcher() {
        return "stats" + SEPARATOR + "dispatcher";
    }
    
    public static String statsBooking() {
        return "stats" + SEPARATOR + "booking";
    }
    
    public static String statsBulkUpdate() {
        return "stats" + SEPARATOR + "bulk_update";
    }
    
    // Lock keys for distributed operations
    public static String lock(String resource) {
        return "lock" + SEPARATOR + resource;
    }
    
    // Facility keys
    public static String facility(String facilityId) {
        return "facility" + SEPARATOR + facilityId;
    }
    
    // Department keys
    public static String department(String deptId) {
        return "dept" + SEPARATOR + deptId;
    }
    
    // Allocation keys
    public static String allocation(String allocId) {
        return "alloc" + SEPARATOR + allocId;
    }
    
    // Group keys
    public static String group(String groupId) {
        return "group" + SEPARATOR + groupId;
    }
    
    // Limit keys
    public static String limit(String limitId) {
        return "limit" + SEPARATOR + limitId;
    }
    
    // Service keys
    public static String service(String serviceName) {
        return "service" + SEPARATOR + serviceName;
    }
    
    public static String serviceHosts(String serviceName) {
        return "service" + SEPARATOR + serviceName + SEPARATOR + "hosts";
    }
    
    // Dependency keys
    public static String frameDependencies(String frameId) {
        return "frame" + SEPARATOR + "deps" + SEPARATOR + frameId;
    }
    
    public static String frameDependenciesPrefix() {
        return "frame" + SEPARATOR + "deps" + SEPARATOR;
    }
    
    public static String frameDependents(String frameId) {
        return "frame" + SEPARATOR + "dependents" + SEPARATOR + frameId;
    }
    
    public static String depend(String dependId) {
        return "depend" + SEPARATOR + dependId;
    }
    
    // Layer frame management
    public static String layerFrames(String layerId) {
        return "layer" + SEPARATOR + "frames" + SEPARATOR + layerId;
    }
    
    public static String jobFrames(String jobId) {
        return "job" + SEPARATOR + "frames" + SEPARATOR + jobId;
    }
}

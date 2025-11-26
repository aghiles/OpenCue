
/*
 * Copyright Contributors to the OpenCue Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.imageworks.spcue.dao.redis;

import com.imageworks.spcue.grpc.job.JobState;

/**
 * Event published when a job's state or resources change.
 * Used to sync job data to Redis for fast scheduling queries.
 */
public class JobStateChangedEvent {

    private final String jobId;
    private final String showId;
    private final String facilityId;
    private final String folderId;
    private final JobState state;
    private final boolean paused;
    private final String os;
    private final int priority;
    private final int cores;
    private final int minCores;
    private final int maxCores;
    private final int gpus;
    private final int maxGpus;
    private final long tsUpdated;
    // Folder resource info
    private final int folderCores;
    private final int folderMaxCores;
    private final int folderGpus;
    private final int folderMaxGpus;
    // DispatchFrame fields
    private final String showName;
    private final String jobName;
    private final String shot;
    private final String owner;
    private final Integer uid;
    private final String logDir;
    private final String lokiURL;

    public JobStateChangedEvent(String jobId, String showId, String facilityId, String folderId,
                                 JobState state, boolean paused, String os,
                                 int priority, int cores, int minCores, int maxCores,
                                 int gpus, int maxGpus, long tsUpdated,
                                 int folderCores, int folderMaxCores,
                                 int folderGpus, int folderMaxGpus,
                                 String showName, String jobName, String shot,
                                 String owner, Integer uid, String logDir, String lokiURL) {
        this.jobId = jobId;
        this.showId = showId;
        this.facilityId = facilityId;
        this.folderId = folderId;
        this.state = state;
        this.paused = paused;
        this.os = os;
        this.priority = priority;
        this.cores = cores;
        this.minCores = minCores;
        this.maxCores = maxCores;
        this.gpus = gpus;
        this.maxGpus = maxGpus;
        this.tsUpdated = tsUpdated;
        this.folderCores = folderCores;
        this.folderMaxCores = folderMaxCores;
        this.folderGpus = folderGpus;
        this.folderMaxGpus = folderMaxGpus;
        this.showName = showName;
        this.jobName = jobName;
        this.shot = shot;
        this.owner = owner;
        this.uid = uid;
        this.logDir = logDir;
        this.lokiURL = lokiURL;
    }

    public String getJobId() {
        return jobId;
    }

    public String getShowId() {
        return showId;
    }

    public String getFacilityId() {
        return facilityId;
    }

    public String getFolderId() {
        return folderId;
    }

    public JobState getState() {
        return state;
    }

    public boolean isPaused() {
        return paused;
    }

    public String getOs() {
        return os;
    }

    public int getPriority() {
        return priority;
    }

    public int getCores() {
        return cores;
    }

    public int getMinCores() {
        return minCores;
    }

    public int getMaxCores() {
        return maxCores;
    }

    public int getGpus() {
        return gpus;
    }

    public int getMaxGpus() {
        return maxGpus;
    }

    public long getTsUpdated() {
        return tsUpdated;
    }

    public int getFolderCores() {
        return folderCores;
    }

    public int getFolderMaxCores() {
        return folderMaxCores;
    }

    public int getFolderGpus() {
        return folderGpus;
    }

    public int getFolderMaxGpus() {
        return folderMaxGpus;
    }

    public String getShowName() {
        return showName;
    }

    public String getJobName() {
        return jobName;
    }

    public String getShot() {
        return shot;
    }

    public String getOwner() {
        return owner;
    }

    public Integer getUid() {
        return uid;
    }

    public String getLogDir() {
        return logDir;
    }

    public String getLokiURL() {
        return lokiURL;
    }

    /**
     * Check if this job is eligible for dispatch.
     */
    public boolean isDispatchable() {
        return state == JobState.PENDING && !paused;
    }
}


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

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.imageworks.spcue.FrameInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.grpc.job.FrameState;
import com.imageworks.spcue.grpc.job.JobState;

/**
 * No-op implementation of SchedulingEventPublisher.
 *
 * Used when Redis scheduling cache is disabled.
 * All methods are no-ops to avoid any overhead.
 */
@Component
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpSchedulingEventPublisher implements SchedulingEventPublisher {

    @Override
    public void publishFrameStateChanged(FrameInterface frame, FrameState previousState,
                                          FrameState newState) {
        // No-op
    }

    @Override
    public void publishFrameStateChanged(String frameId, String layerId, String jobId,
                                          FrameState previousState, FrameState newState,
                                          int dispatchOrder, int layerOrder) {
        // No-op
    }

    @Override
    public void publishLayerUpdated(LayerInterface layer) {
        // No-op
    }

    @Override
    public void publishJobCompleted(String jobId, String showId, String facilityId) {
        // No-op
    }

    @Override
    public void publishJobStateChanged(String jobId, String showId, String facilityId, String folderId,
                                        JobState state, boolean paused, String os,
                                        int priority, int cores, int minCores, int maxCores,
                                        int gpus, int maxGpus, long tsUpdated,
                                        int folderCores, int folderMaxCores,
                                        int folderGpus, int folderMaxGpus) {
        // No-op
    }
}

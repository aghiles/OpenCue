
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

import com.imageworks.spcue.FrameInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Interface for publishing scheduling-related events.
 *
 * This abstraction allows frame state changes to be propagated to
 * external caching systems (like Redis) without coupling the DAO
 * layer to specific implementations.
 *
 * When Redis is disabled, a no-op implementation is used.
 * When Redis is enabled, events are published for cache synchronization.
 */
public interface SchedulingEventPublisher {

    /**
     * Publish a frame state change event.
     *
     * @param frame         The frame that changed
     * @param previousState The previous state (may be null for new frames)
     * @param newState      The new state
     */
    void publishFrameStateChanged(FrameInterface frame, FrameState previousState, FrameState newState);

    /**
     * Publish a frame state change with dispatch order info.
     *
     * @param frameId        Frame ID
     * @param layerId        Layer ID
     * @param jobId          Job ID
     * @param previousState  Previous state
     * @param newState       New state
     * @param dispatchOrder  Frame dispatch order
     * @param layerOrder     Layer order
     */
    void publishFrameStateChanged(String frameId, String layerId, String jobId,
                                   FrameState previousState, FrameState newState,
                                   int dispatchOrder, int layerOrder);

    /**
     * Publish a layer update event.
     *
     * @param layer The layer that was updated
     */
    void publishLayerUpdated(LayerInterface layer);

    /**
     * Notify that a job has completed and its cache data can be cleaned up.
     *
     * @param jobId The job ID
     */
    void publishJobCompleted(String jobId);
}

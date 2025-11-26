
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import com.imageworks.spcue.FrameInterface;
import com.imageworks.spcue.LayerInterface;
import com.imageworks.spcue.grpc.job.FrameState;

/**
 * Redis-enabled implementation of SchedulingEventPublisher.
 *
 * Publishes scheduling events to Spring's ApplicationEventPublisher,
 * which are then handled by RedisSchedulingEventListener after the
 * transaction commits.
 *
 * This decouples the DAO layer from Redis implementation details.
 */
@Component
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisSchedulingEventPublisher implements SchedulingEventPublisher {

    private static final Logger logger = LogManager.getLogger(RedisSchedulingEventPublisher.class);

    private final ApplicationEventPublisher eventPublisher;

    public RedisSchedulingEventPublisher(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
        logger.info("Redis scheduling event publisher initialized");
    }

    @Override
    public void publishFrameStateChanged(FrameInterface frame, FrameState previousState,
                                          FrameState newState) {
        // For basic frame interface, we don't have dispatch order info
        // Use 0 as default - the initializer will set correct values
        publishFrameStateChanged(
                frame.getFrameId(),
                frame.getLayerId(),
                frame.getJobId(),
                previousState,
                newState,
                0,
                0
        );
    }

    @Override
    public void publishFrameStateChanged(String frameId, String layerId, String jobId,
                                          FrameState previousState, FrameState newState,
                                          int dispatchOrder, int layerOrder) {
        logger.trace("Publishing frame state change: {} {} -> {}",
                frameId, previousState, newState);

        FrameStateChangedEvent event = new FrameStateChangedEvent(
                frameId, layerId, jobId,
                previousState, newState,
                dispatchOrder, layerOrder
        );

        eventPublisher.publishEvent(event);
    }

    @Override
    public void publishLayerUpdated(LayerInterface layer) {
        // LayerUpdatedEvent requires more details - fetch them or defer to initializer
        logger.trace("Layer updated event for: {}", layer.getLayerId());
        // For now, we rely on the initializer to populate layer data
        // Full layer events would require fetching layer details
    }

    @Override
    public void publishJobCompleted(String jobId) {
        logger.debug("Publishing job completed: {}", jobId);
        eventPublisher.publishEvent(new JobCompletedEvent(jobId));
    }

    /**
     * Event published when a job completes.
     */
    public static class JobCompletedEvent {
        private final String jobId;

        public JobCompletedEvent(String jobId) {
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }
    }
}

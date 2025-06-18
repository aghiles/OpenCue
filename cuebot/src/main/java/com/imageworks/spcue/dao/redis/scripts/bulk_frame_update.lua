-- bulk_frame_update.lua
-- Bulk update frames for job/layer operations
-- Now handles layer completion events

-- KEYS[1]: frame:* prefix
-- KEYS[2]: dispatch:queue:{facility_id} (sorted set)
-- KEYS[3]: booking:* prefix
-- ARGV[1]: new state (e.g., 'WAITING', 'DEAD', etc.)
-- ARGV[2]: timestamp
-- ARGV[3+]: frame IDs

local frame_prefix = KEYS[1]
local queue_key = KEYS[2]
local booking_prefix = KEYS[3]

local new_state = ARGV[1]
local timestamp = ARGV[2]
local updated_count = 0

-- Track layer completion
local layers_to_check = {}

-- Process each frame ID
for i = 3, #ARGV do
    local frame_id = ARGV[i]
    local frame_key = frame_prefix .. frame_id
    
    -- Check if frame exists
    if redis.call('EXISTS', frame_key) == 1 then
        local current_state = redis.call('HGET', frame_key, 'state')
        local layer_id = redis.call('HGET', frame_key, 'layer_id')
        
        -- Different logic based on target state
        if new_state == 'WAITING' then
            -- Transitioning frames to WAITING (e.g., retry operation)
            if current_state == 'DEAD' or current_state == 'DEPEND' then
                -- First check if layer is active
                local layer_active = true
                if layer_id then
                    local layer_key = 'layer:' .. layer_id
                    local active_flag = redis.call('HGET', layer_key, 'active')
                    if active_flag ~= 'true' then
                        layer_active = false
                    end
                end
                
                if layer_active then
                    -- Check frame-specific dependencies
                    local deps_satisfied = true
                    local deps_key = 'frame:deps:' .. frame_id
                    local depends_on_count = redis.call('SCARD', deps_key)
                    
                    if depends_on_count > 0 then
                        local remaining_deps = redis.call('SMEMBERS', deps_key)
                        for _, dep_frame_id in ipairs(remaining_deps) do
                            local dep_frame_key = frame_prefix .. dep_frame_id
                            local dep_state = redis.call('HGET', dep_frame_key, 'state')
                            if dep_state ~= 'SUCCEEDED' and dep_state ~= 'EATEN' then
                                deps_satisfied = false
                                break
                            end
                        end
                    end
                    
                    if deps_satisfied then
                        -- Update frame state
                        redis.call('HMSET', frame_key,
                            'state', 'WAITING',
                            'state_time', timestamp
                        )
                        
                        -- Clear failure data
                        redis.call('HDEL', frame_key, 
                            'exit_status', 
                            'stop_time', 
                            'proc_id', 
                            'host_id', 
                            'host_name'
                        )
                        
                        -- Add back to dispatch queue
                        local priority = tonumber(redis.call('HGET', frame_key, 'priority') or '0')
                        local job_priority = tonumber(redis.call('HGET', frame_key, 'job_priority') or '0')
                        local layer_order = tonumber(redis.call('HGET', frame_key, 'layer_order') or '0')
                        local frame_number = tonumber(redis.call('HGET', frame_key, 'frame_number') or '0')
                        
                        -- Calculate dispatch score
                        local score = (job_priority * 1000000) + 
                                     (priority * 10000) + 
                                     ((100 - layer_order) * 100) + 
                                     (10000 - frame_number)
                        
                        redis.call('ZADD', queue_key, score, frame_id)
                        updated_count = updated_count + 1
                    else
                        -- Dependencies not satisfied, set to DEPEND state
                        redis.call('HMSET', frame_key,
                            'state', 'DEPEND',
                            'state_time', timestamp
                        )
                        updated_count = updated_count + 1
                    end
                else
                    -- Layer not active, set to DEPEND state
                    redis.call('HMSET', frame_key,
                        'state', 'DEPEND',
                        'state_time', timestamp
                    )
                    updated_count = updated_count + 1
                end
            end
            
        elseif new_state == 'DEAD' then
            -- Killing frames
            if current_state == 'WAITING' or current_state == 'RUNNING' then
                -- Update frame state
                redis.call('HMSET', frame_key,
                    'state', 'DEAD',
                    'state_time', timestamp,
                    'exit_status', '-1',
                    'stop_time', timestamp
                )
                
                -- Remove from dispatch queue
                redis.call('ZREM', queue_key, frame_id)
                
                -- Clear any booking
                local booking_key = booking_prefix .. frame_id
                if redis.call('EXISTS', booking_key) == 1 then
                    local proc_id = redis.call('GET', booking_key)
                    redis.call('DEL', booking_key)
                    
                    -- Clear proc assignment if exists
                    if proc_id then
                        local proc_key = 'proc:' .. proc_id
                        if redis.call('EXISTS', proc_key) == 1 then
                            redis.call('HDEL', proc_key, 'frame_id', 'job_id', 'show_id')
                        end
                    end
                end
                
                updated_count = updated_count + 1
            end
            
        elseif new_state == 'EATEN' then
            -- Marking frames as eaten (skipped)
            if current_state == 'WAITING' or current_state == 'DEPEND' then
                redis.call('HMSET', frame_key,
                    'state', 'EATEN',
                    'state_time', timestamp
                )
                
                -- Remove from dispatch queue
                redis.call('ZREM', queue_key, frame_id)
                
                -- Update dependent frames
                local dependents_key = 'frame:dependents:' .. frame_id
                local dependents = redis.call('SMEMBERS', dependents_key)
                
                for _, dependent_frame_id in ipairs(dependents) do
                    -- Remove this frame from dependent's dependency set
                    local dep_key = 'frame:deps:' .. dependent_frame_id
                    redis.call('SREM', dep_key, frame_id)
                    
                    -- Check if dependent can now be made WAITING
                    local remaining_deps = redis.call('SCARD', dep_key)
                    if remaining_deps == 0 then
                        local dep_frame_key = frame_prefix .. dependent_frame_id
                        local dep_state = redis.call('HGET', dep_frame_key, 'state')
                        
                        if dep_state == 'DEPEND' then
                            -- Check if dependent's layer is active
                            local dep_layer_id = redis.call('HGET', dep_frame_key, 'layer_id')
                            local dep_layer_active = true
                            
                            if dep_layer_id then
                                local dep_layer_key = 'layer:' .. dep_layer_id
                                local active_flag = redis.call('HGET', dep_layer_key, 'active')
                                if active_flag ~= 'true' then
                                    dep_layer_active = false
                                end
                            end
                            
                            if dep_layer_active then
                                -- Update to WAITING and add to queue
                                redis.call('HSET', dep_frame_key, 'state', 'WAITING')
                                
                                local dep_priority = tonumber(redis.call('HGET', dep_frame_key, 'priority') or '0')
                                local dep_job_priority = tonumber(redis.call('HGET', dep_frame_key, 'job_priority') or '0')
                                local dep_layer_order = tonumber(redis.call('HGET', dep_frame_key, 'layer_order') or '0')
                                local dep_frame_number = tonumber(redis.call('HGET', dep_frame_key, 'frame_number') or '0')
                                
                                local dep_score = (dep_job_priority * 1000000) + 
                                                (dep_priority * 10000) + 
                                                ((100 - dep_layer_order) * 100) + 
                                                (10000 - dep_frame_number)
                                
                                redis.call('ZADD', queue_key, dep_score, dependent_frame_id)
                            end
                        end
                    end
                end
                
                -- Track layer for completion check
                if layer_id then
                    layers_to_check[layer_id] = true
                end
                
                updated_count = updated_count + 1
            end
            
        elseif new_state == 'SUCCEEDED' then
            -- Frame completed successfully
            if current_state == 'RUNNING' then
                redis.call('HMSET', frame_key,
                    'state', 'SUCCEEDED',
                    'state_time', timestamp
                )
                
                -- Clear booking
                local booking_key = booking_prefix .. frame_id
                redis.call('DEL', booking_key)
                
                -- Update dependent frames
                local dependents_key = 'frame:dependents:' .. frame_id
                local dependents = redis.call('SMEMBERS', dependents_key)
                
                for _, dependent_frame_id in ipairs(dependents) do
                    -- Remove this frame from dependent's dependency set
                    local dep_key = 'frame:deps:' .. dependent_frame_id
                    redis.call('SREM', dep_key, frame_id)
                    
                    -- Check if dependent can now be made WAITING
                    local remaining_deps = redis.call('SCARD', dep_key)
                    if remaining_deps == 0 then
                        local dep_frame_key = frame_prefix .. dependent_frame_id
                        local dep_state = redis.call('HGET', dep_frame_key, 'state')
                        
                        if dep_state == 'DEPEND' then
                            -- Check if dependent's layer is active
                            local dep_layer_id = redis.call('HGET', dep_frame_key, 'layer_id')
                            local dep_layer_active = true
                            
                            if dep_layer_id then
                                local dep_layer_key = 'layer:' .. dep_layer_id
                                local active_flag = redis.call('HGET', dep_layer_key, 'active')
                                if active_flag ~= 'true' then
                                    dep_layer_active = false
                                end
                            end
                            
                            if dep_layer_active then
                                -- Update to WAITING and add to queue
                                redis.call('HSET', dep_frame_key, 'state', 'WAITING')
                                
                                local dep_priority = tonumber(redis.call('HGET', dep_frame_key, 'priority') or '0')
                                local dep_job_priority = tonumber(redis.call('HGET', dep_frame_key, 'job_priority') or '0')
                                local dep_layer_order = tonumber(redis.call('HGET', dep_frame_key, 'layer_order') or '0')
                                local dep_frame_number = tonumber(redis.call('HGET', dep_frame_key, 'frame_number') or '0')
                                
                                local dep_score = (dep_job_priority * 1000000) + 
                                                (dep_priority * 10000) + 
                                                ((100 - dep_layer_order) * 100) + 
                                                (10000 - dep_frame_number)
                                
                                redis.call('ZADD', queue_key, dep_score, dependent_frame_id)
                            end
                        end
                    end
                end
                
                -- Track layer for completion check
                if layer_id then
                    layers_to_check[layer_id] = true
                end
                
                updated_count = updated_count + 1
            end
            
        else
            -- Generic state update
            redis.call('HMSET', frame_key,
                'state', new_state,
                'state_time', timestamp
            )
            updated_count = updated_count + 1
        end
    end
end

-- Check if any layers are now complete
for layer_id, _ in pairs(layers_to_check) do
    local stats_key = 'stats:layer:' .. layer_id
    local total = tonumber(redis.call('HGET', stats_key, 'total_frames') or '0')
    local succeeded = tonumber(redis.call('HGET', stats_key, 'succeeded_frames') or '0')
    local eaten = tonumber(redis.call('HGET', stats_key, 'eaten_frames') or '0')
    
    if total > 0 and (succeeded + eaten) >= total then
        -- Layer is complete!
        redis.call('HSET', 'layer:' .. layer_id, 'is_complete', 'true')
        redis.call('HSET', 'layer:' .. layer_id, 'complete_time', timestamp)
        
        -- This will trigger layer dependency checks in DependencyDaoRedis
        redis.call('PUBLISH', 'layer:complete', layer_id)
    end
end

-- Update statistics
redis.call('HINCRBY', 'stats:bulk_update', 'total_operations', 1)
redis.call('HINCRBY', 'stats:bulk_update', 'total_frames_updated', updated_count)
redis.call('HSET', 'stats:bulk_update', 'last_update_time', timestamp)
redis.call('HSET', 'stats:bulk_update', 'last_update_state', new_state)
redis.call('HSET', 'stats:bulk_update', 'last_update_count', updated_count)

return updated_count

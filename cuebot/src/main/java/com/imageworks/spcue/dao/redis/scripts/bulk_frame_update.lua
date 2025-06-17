-- bulk_frame_update.lua
-- Bulk update frames for job/layer operations

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

-- Process each frame ID
for i = 3, #ARGV do
    local frame_id = ARGV[i]
    local frame_key = frame_prefix .. frame_id
    
    -- Check if frame exists
    if redis.call('EXISTS', frame_key) == 1 then
        local current_state = redis.call('HGET', frame_key, 'state')
        
        -- Different logic based on target state
        if new_state == 'WAITING' then
            -- Transitioning frames to WAITING (e.g., retry operation)
            if current_state == 'DEAD' or current_state == 'DEPEND' then
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

-- Update statistics
redis.call('HINCRBY', 'stats:bulk_update', 'total_operations', 1)
redis.call('HINCRBY', 'stats:bulk_update', 'total_frames_updated', updated_count)
redis.call('HSET', 'stats:bulk_update', 'last_update_time', timestamp)
redis.call('HSET', 'stats:bulk_update', 'last_update_state', new_state)
redis.call('HSET', 'stats:bulk_update', 'last_update_count', updated_count)

return updated_count

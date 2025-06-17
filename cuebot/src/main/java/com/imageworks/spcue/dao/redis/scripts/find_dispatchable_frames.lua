-- find_dispatchable_frames.lua
-- Finds frames ready for dispatch based on resource requirements and dependencies
-- Replaces complex SQL query with 6+ JOINs

-- KEYS[1]: dispatch:queue:{facility_id} (sorted set)
-- KEYS[2]: booking:* prefix
-- KEYS[3]: frame:* prefix  
-- KEYS[4]: layer:services:* prefix
-- ARGV[1]: available cores
-- ARGV[2]: available memory (KB)
-- ARGV[3]: available gpus
-- ARGV[4]: limit (number of frames to return)
-- ARGV[5+]: available services

local queue_key = KEYS[1]
local booking_prefix = KEYS[2]
local frame_prefix = KEYS[3]
local service_prefix = KEYS[4]

local available_cores = tonumber(ARGV[1])
local available_memory = tonumber(ARGV[2])
local available_gpus = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])

-- Build service lookup table for O(1) access
local available_services = {}
for i = 5, #ARGV do
    available_services[ARGV[i]] = true
end

-- JSON encoding function
local function table_to_json(t)
    local parts = {}
    for k, v in pairs(t) do
        local key = '"' .. k .. '"'
        local value
        if type(v) == "string" then
            value = '"' .. v:gsub('"', '\\"') .. '"'
        elseif type(v) == "boolean" then
            value = tostring(v)
        elseif type(v) == "number" then
            value = tostring(v)
        else
            value = "null"
        end
        table.insert(parts, key .. ':' .. value)
    end
    return '{' .. table.concat(parts, ',') .. '}'
end

local results = {}
local checked = 0
local max_to_check = limit * 10 -- Check up to 10x to account for filtering

-- Get frame IDs from priority queue (highest priority first)
local frame_ids = redis.call('ZREVRANGE', queue_key, 0, max_to_check - 1)

for _, frame_id in ipairs(frame_ids) do
    -- Check if frame is already booked
    local booking_key = booking_prefix .. frame_id
    if redis.call('EXISTS', booking_key) == 0 then
        
        -- Get frame data
        local frame_key = frame_prefix .. frame_id
        local frame_data = redis.call('HGETALL', frame_key)
        
        -- Convert to table
        local frame = {}
        for i = 1, #frame_data, 2 do
            frame[frame_data[i]] = frame_data[i + 1]
        end
        
        -- Validate frame exists and has required fields
        if frame.state and frame.cores_min and frame.memory_min then
            
            -- Check frame state and job status
            if frame.state == 'WAITING' and frame.job_active == 'true' and frame.layer_enabled == 'true' then
                
                -- Check resource requirements
                local cores_min = tonumber(frame.cores_min) or 0
                local memory_min = tonumber(frame.memory_min) or 0
                local gpu_min = tonumber(frame.gpu_min) or 0
                
                if cores_min <= available_cores and 
                   memory_min <= available_memory and 
                   gpu_min <= available_gpus then
                    
                    -- Check service requirements
                    local service_match = true
                    if frame.layer_id and frame.has_services == 'true' then
                        local service_key = service_prefix .. frame.layer_id
                        local required_services = redis.call('SMEMBERS', service_key)
                        
                        -- Check if all required services are available
                        for _, service in ipairs(required_services) do
                            if not available_services[service] then
                                service_match = false
                                break
                            end
                        end
                    end
                    
                    -- Check show active status
                    local show_active = frame.show_active ~= 'false'
                    
                    -- Check if not paused
                    local not_paused = frame.job_paused ~= 'true'
                    
                    if service_match and show_active and not_paused then
                        -- NEW: Check dependencies
                        local deps_satisfied = true
                        local deps_key = 'frame:deps:' .. frame_id
                        local depends_on_count = redis.call('SCARD', deps_key)
                        
                        if depends_on_count > 0 then
                            -- Has dependencies, need to check them
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
                            -- Frame is dispatchable! Add to results
                            frame.frame_id = frame_id
                            
                            -- Include dispatch-relevant fields in result
                            local dispatch_frame = {
                                frame_id = frame_id,
                                frame_name = frame.frame_name,
                                job_id = frame.job_id,
                                job_name = frame.job_name,
                                layer_id = frame.layer_id,
                                layer_name = frame.layer_name,
                                show_id = frame.show_id,
                                facility_id = frame.facility_id,
                                priority = tonumber(frame.priority) or 0,
                                cores_min = cores_min,
                                memory_min = memory_min,
                                gpu_min = gpu_min,
                                state = frame.state,
                                frame_number = tonumber(frame.frame_number) or 0,
                                layer_order = tonumber(frame.layer_order) or 0,
                                command = frame.command,
                                services = frame.services
                            }
                            
                            -- Convert to JSON and add to results
                            local frame_json = table_to_json(dispatch_frame)
                            table.insert(results, frame_json)
                            
                            if #results >= limit then
                                break
                            end
                        end
                    end
                end
            end
        end
    end
    
    checked = checked + 1
    if checked >= max_to_check then
        break
    end
end

-- Log statistics for monitoring
if #results < limit and checked >= max_to_check then
    redis.call('HINCRBY', 'stats:dispatcher', 'insufficient_frames', 1)
end

redis.call('HSET', 'stats:dispatcher', 'last_query_checked', checked)
redis.call('HSET', 'stats:dispatcher', 'last_query_returned', #results)
redis.call('HSET', 'stats:dispatcher', 'last_query_time', redis.call('TIME')[1])

return results

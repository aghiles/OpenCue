-- atomic_booking.lua
-- Atomically book a frame to a proc/host, preventing double-booking

-- KEYS[1]: frame:{frame_id} (hash)
-- KEYS[2]: booking:{frame_id} (string)
-- KEYS[3]: proc:{proc_id} (hash)
-- KEYS[4]: dispatch:queue:* prefix
-- ARGV[1]: frame_id
-- ARGV[2]: proc_id
-- ARGV[3]: host_id
-- ARGV[4]: timestamp

local frame_key = KEYS[1]
local booking_key = KEYS[2]
local proc_key = KEYS[3]
local queue_prefix = KEYS[4]

local frame_id = ARGV[1]
local proc_id = ARGV[2]
local host_id = ARGV[3]
local timestamp = ARGV[4]

-- Check if frame is already booked
if redis.call('EXISTS', booking_key) == 1 then
    -- Frame is already booked
    return false
end

-- Get frame data
local frame_state = redis.call('HGET', frame_key, 'state')
local facility_id = redis.call('HGET', frame_key, 'facility_id')

-- Verify frame is in WAITING state
if frame_state ~= 'WAITING' then
    -- Frame is not available for booking
    return false
end

-- NEW: Verify dependencies are satisfied
local deps_key = 'frame:deps:' .. frame_id
local depends_on_count = redis.call('SCARD', deps_key)

if depends_on_count > 0 then
    -- Has dependencies, verify they're all satisfied
    local remaining_deps = redis.call('SMEMBERS', deps_key)
    for _, dep_frame_id in ipairs(remaining_deps) do
        local dep_frame_key = 'frame:' .. dep_frame_id
        local dep_state = redis.call('HGET', dep_frame_key, 'state')
        if dep_state ~= 'SUCCEEDED' and dep_state ~= 'EATEN' then
            -- Dependency not satisfied, cannot book
            redis.call('HINCRBY', 'stats:booking', 'dependency_blocks', 1)
            return false
        end
    end
end

-- Atomic booking operation
-- 1. Create booking record (with TTL to prevent stuck bookings)
redis.call('SETEX', booking_key, 86400, proc_id) -- 24 hour TTL

-- 2. Update frame state and assignment
redis.call('HMSET', frame_key,
    'state', 'RUNNING',
    'proc_id', proc_id,
    'host_id', host_id,
    'host_name', redis.call('HGET', proc_key, 'host_name') or '',
    'start_time', timestamp,
    'attempt', tostring((tonumber(redis.call('HGET', frame_key, 'attempt') or '0')) + 1)
)

-- 3. Update proc assignment
redis.call('HMSET', proc_key,
    'frame_id', frame_id,
    'job_id', redis.call('HGET', frame_key, 'job_id') or '',
    'show_id', redis.call('HGET', frame_key, 'show_id') or '',
    'assigned_time', timestamp
)

-- 4. Remove from dispatch queue
if facility_id then
    local queue_key = queue_prefix .. facility_id
    redis.call('ZREM', queue_key, frame_id)
end

-- 5. Update stats
redis.call('HINCRBY', 'stats:booking', 'total_bookings', 1)
redis.call('HSET', 'stats:booking', 'last_booking_time', timestamp)

-- Booking successful
return true

--[[
  Find next dispatch frames for a job and host.

  This Lua script mirrors the SQL query FIND_DISPATCH_FRAME_BY_JOB_AND_HOST.
  It atomically finds waiting frames that match host resource requirements.

  KEYS:
    KEYS[1] = layers:waiting:{jobId} - Set of layer IDs with waiting frames

  ARGV:
    ARGV[1] = hostCores     - Available cores on host
    ARGV[2] = hostMemory    - Available memory on host (bytes)
    ARGV[3] = hostGpus      - Available GPUs on host
    ARGV[4] = hostGpuMemory - Available GPU memory on host (bytes)
    ARGV[5] = hostTags      - Comma-separated list of host tags
    ARGV[6] = threadMode    - 0 = AUTO, 1 = ALL (for threadable check)
    ARGV[7] = limit         - Maximum number of frames to return
    ARGV[8] = noGpu         - 1 = skip GPU checks (NO_GPU mode), 0 = normal

  Returns:
    List of frame IDs that match the criteria, ordered by dispatch order
]]

local layersWaitingKey = KEYS[1]
local hostCores = tonumber(ARGV[1])
local hostMemory = tonumber(ARGV[2])
local hostGpus = tonumber(ARGV[3])
local hostGpuMemory = tonumber(ARGV[4])
local hostTags = ARGV[5]
local threadMode = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local noGpu = tonumber(ARGV[8] or 0)

-- Helper function to check if host tags match layer tags
-- Layer tags use regex pattern, host tags is a space/comma separated list
local function tagsMatch(hostTagStr, layerTagPattern)
    if layerTagPattern == nil or layerTagPattern == '' then
        return true
    end

    -- Simple pattern matching: check if any host tag matches the layer pattern
    -- In production, this would need proper regex support
    -- For PoC, we do a simple substring/contains check
    local hostTagsLower = string.lower(hostTagStr)
    local patternLower = string.lower(layerTagPattern)

    -- Split pattern by | (OR in regex)
    for pattern in string.gmatch(patternLower, "[^|]+") do
        pattern = pattern:gsub("^%s*(.-)%s*$", "%1") -- trim
        if string.find(hostTagsLower, pattern, 1, true) then
            return true
        end
    end

    return false
end

-- Get all layer IDs that have waiting frames for this job
local layerIds = redis.call('SMEMBERS', layersWaitingKey)

if #layerIds == 0 then
    return {}
end

local eligibleFrames = {}

-- Check each layer for resource compatibility
for _, layerId in ipairs(layerIds) do
    local layerKey = 'layer:' .. layerId
    local layerData = redis.call('HGETALL', layerKey)

    if #layerData > 0 then
        -- Parse layer data into table
        local layer = {}
        for i = 1, #layerData, 2 do
            layer[layerData[i]] = layerData[i + 1]
        end

        local minCores = tonumber(layer['minCores'] or 0)
        local minMemory = tonumber(layer['minMemory'] or 0)
        local minGpus = tonumber(layer['minGpus'] or 0)
        local minGpuMemory = tonumber(layer['minGpuMemory'] or 0)
        local threadable = layer['threadable'] == 'true'
        local layerTags = layer['tags'] or ''

        -- Check resource requirements
        local resourcesMatch = true

        -- Check cores
        if minCores > hostCores then
            resourcesMatch = false
        end

        -- Check memory
        if minMemory > hostMemory then
            resourcesMatch = false
        end

        -- GPU checks (skip if noGpu mode)
        if noGpu == 0 then
            -- Check GPUs
            if minGpus > hostGpus then
                resourcesMatch = false
            end

            -- Check GPU memory (BETWEEN minGpuMem AND hostGpuMem)
            if hostGpuMemory > 0 then
                if minGpuMemory < 1 or minGpuMemory > hostGpuMemory then
                    resourcesMatch = false
                end
            elseif minGpuMemory > 0 then
                resourcesMatch = false
            end
        end

        -- Check threadable requirement
        -- threadMode: 0 = AUTO (requires threadable), 1 = ALL (any)
        if threadMode == 0 and not threadable then
            resourcesMatch = false
        end

        -- Check tags
        if resourcesMatch and not tagsMatch(hostTags, layerTags) then
            resourcesMatch = false
        end

        -- If layer matches, get its waiting frames
        if resourcesMatch then
            local framesWaitingKey = 'frames:waiting:' .. layerId
            -- Get frames ordered by score (dispatch order)
            local frames = redis.call('ZRANGE', framesWaitingKey, 0, limit - 1)

            for _, frameId in ipairs(frames) do
                table.insert(eligibleFrames, {
                    frameId = frameId,
                    layerId = layerId,
                    -- Get the score for sorting
                    score = redis.call('ZSCORE', framesWaitingKey, frameId)
                })
            end
        end
    end
end

-- Sort all eligible frames by score
table.sort(eligibleFrames, function(a, b)
    return tonumber(a.score) < tonumber(b.score)
end)

-- Return only frame IDs, limited to requested count
local result = {}
for i = 1, math.min(#eligibleFrames, limit) do
    table.insert(result, eligibleFrames[i].frameId)
end

return result

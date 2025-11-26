--[[
  Find next dispatch frames for a specific layer and host.

  This Lua script mirrors the SQL query FIND_DISPATCH_FRAME_BY_LAYER_AND_HOST.
  Simpler than the job version - only checks one layer instead of iterating.

  KEYS:
    KEYS[1] = layer:{layerId} - Layer metadata hash
    KEYS[2] = frames:waiting:{layerId} - Sorted set of waiting frame IDs

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

local layerKey = KEYS[1]
local framesWaitingKey = KEYS[2]
local hostCores = tonumber(ARGV[1])
local hostMemory = tonumber(ARGV[2])
local hostGpus = tonumber(ARGV[3])
local hostGpuMemory = tonumber(ARGV[4])
local hostTags = ARGV[5]
local threadMode = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local noGpu = tonumber(ARGV[8] or 0)

-- Helper function to check if host tags match layer tags
local function tagsMatch(hostTagStr, layerTagPattern)
    if layerTagPattern == nil or layerTagPattern == '' then
        return true
    end

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

-- Get layer data
local layerData = redis.call('HGETALL', layerKey)

if #layerData == 0 then
    return {}
end

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

    -- Check GPU memory
    if hostGpuMemory > 0 then
        if minGpuMemory < 1 or minGpuMemory > hostGpuMemory then
            resourcesMatch = false
        end
    elseif minGpuMemory > 0 then
        resourcesMatch = false
    end
end

-- Check threadable requirement
if threadMode == 0 and not threadable then
    resourcesMatch = false
end

-- Check tags
if resourcesMatch and not tagsMatch(hostTags, layerTags) then
    resourcesMatch = false
end

-- If layer doesn't match, return empty
if not resourcesMatch then
    return {}
end

-- Layer matches - return waiting frames ordered by score
return redis.call('ZRANGE', framesWaitingKey, 0, limit - 1)

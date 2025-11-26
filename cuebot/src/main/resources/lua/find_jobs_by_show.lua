--[[
  Find jobs by show for dispatch.

  This Lua script mirrors the SQL query FIND_JOBS_BY_SHOW.
  It finds pending jobs with waiting frames that match host resources,
  ordered by calculated priority.

  Priority calculation:
    priority + (100 * (1 - cores/min_cores)) + age_in_days

  KEYS:
    KEYS[1] = jobs:pending:{showId}:{facilityId} - Set of pending job IDs

  ARGV:
    ARGV[1]  = hostCores      - Available cores on host
    ARGV[2]  = hostMemory     - Available memory on host (bytes)
    ARGV[3]  = threadMode     - 0 = AUTO (requires threadable), 1 = ALL
    ARGV[4]  = hostGpus       - Available GPUs on host (for GPU filter)
    ARGV[5]  = minGpuMem      - Minimum GPU memory filter
    ARGV[6]  = maxGpuMem      - Maximum GPU memory filter
    ARGV[7]  = hostTags       - Comma-separated list of host tags
    ARGV[8]  = hostOs         - Comma-separated list of supported OS
    ARGV[9]  = currentTime    - Current timestamp in seconds
    ARGV[10] = limit          - Maximum number of jobs to return

  Returns:
    List of {jobId, priority, rank} tables ordered by priority descending
]]

local jobsPendingKey = KEYS[1]
local hostCores = tonumber(ARGV[1])
local hostMemory = tonumber(ARGV[2])
local threadMode = tonumber(ARGV[3])
local hostGpus = tonumber(ARGV[4])
local minGpuMem = tonumber(ARGV[5])
local maxGpuMem = tonumber(ARGV[6])
local hostTags = ARGV[7]
local hostOs = ARGV[8]
local currentTime = tonumber(ARGV[9])
local limit = tonumber(ARGV[10])

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

-- Helper function to check OS compatibility
local function osMatches(jobOs, hostOsList)
    if jobOs == nil or jobOs == '' then
        return true
    end

    local jobOsLower = string.lower(jobOs)
    local hostOsLower = string.lower(hostOsList)

    for os in string.gmatch(hostOsLower, "[^,]+") do
        os = os:gsub("^%s*(.-)%s*$", "%1") -- trim
        if os == jobOsLower then
            return true
        end
    end

    return false
end

-- Get all pending job IDs for this show/facility
local jobIds = redis.call('SMEMBERS', jobsPendingKey)

if #jobIds == 0 then
    return {}
end

local eligibleJobs = {}

-- Check each job
for _, jobId in ipairs(jobIds) do
    local jobKey = 'job:' .. jobId
    local jobData = redis.call('HGETALL', jobKey)

    if #jobData > 0 then
        -- Parse job data into table
        local job = {}
        for i = 1, #jobData, 2 do
            job[jobData[i]] = jobData[i + 1]
        end

        -- Check if job is paused
        if job['paused'] ~= 'true' then
            -- Check OS compatibility
            if osMatches(job['os'], hostOs) then
                -- Check folder resource limits
                local folderMaxCores = tonumber(job['folderMaxCores'] or -1)
                local folderCores = tonumber(job['folderCores'] or 0)

                -- Get job's layers with waiting frames
                local layersKey = 'job:layers:waiting:' .. jobId
                local layerIds = redis.call('SMEMBERS', layersKey)

                local hasEligibleLayer = false

                for _, layerId in ipairs(layerIds) do
                    local layerKey = 'layer:' .. layerId
                    local layerData = redis.call('HGETALL', layerKey)

                    if #layerData > 0 then
                        local layer = {}
                        for i = 1, #layerData, 2 do
                            layer[layerData[i]] = layerData[i + 1]
                        end

                        local minCores = tonumber(layer['minCores'] or 0)
                        local minMemory = tonumber(layer['minMemory'] or 0)
                        local minGpus = tonumber(layer['minGpus'] or 0)
                        local layerGpuMem = tonumber(layer['minGpuMemory'] or 0)
                        local threadable = layer['threadable'] == 'true'
                        local layerTags = layer['tags'] or ''

                        -- Check resource requirements
                        local layerMatches = true

                        -- Check cores
                        if minCores > hostCores then
                            layerMatches = false
                        end

                        -- Check memory
                        if minMemory > hostMemory then
                            layerMatches = false
                        end

                        -- Check threadable
                        if threadMode == 0 and not threadable then
                            layerMatches = false
                        end

                        -- Check GPUs (BETWEEN 1 AND hostGpus)
                        if hostGpus > 0 then
                            if minGpus < 1 or minGpus > hostGpus then
                                layerMatches = false
                            end
                        elseif minGpus > 0 then
                            layerMatches = false
                        end

                        -- Check GPU memory (BETWEEN minGpuMem AND maxGpuMem)
                        if maxGpuMem > 0 then
                            if layerGpuMem < minGpuMem or layerGpuMem > maxGpuMem then
                                layerMatches = false
                            end
                        end

                        -- Check tags
                        if layerMatches and not tagsMatch(hostTags, layerTags) then
                            layerMatches = false
                        end

                        -- Check folder resource limits
                        if layerMatches and folderMaxCores ~= -1 then
                            if folderCores + minCores >= folderMaxCores then
                                layerMatches = false
                            end
                        end

                        -- Check job resource limits
                        local jobMaxCores = tonumber(job['maxCores'] or -1)
                        local jobCores = tonumber(job['cores'] or 0)
                        if layerMatches and jobMaxCores ~= -1 then
                            if jobCores + minCores > jobMaxCores then
                                layerMatches = false
                            end
                        end

                        if layerMatches then
                            hasEligibleLayer = true
                            break
                        end
                    end
                end

                if hasEligibleLayer then
                    -- Calculate priority score
                    local basePriority = tonumber(job['priority'] or 0)
                    local jobCores = tonumber(job['cores'] or 0)
                    local minCores = tonumber(job['minCores'] or 0)
                    local tsUpdated = tonumber(job['tsUpdated'] or currentTime)

                    -- Under-proc bonus: 100 * (1 - cores/min_cores)
                    local underProcBonus = 0
                    if minCores > 0 then
                        if jobCores < minCores then
                            underProcBonus = 100 * (1 - jobCores / minCores)
                        end
                    end

                    -- Age bonus: days since last update
                    local ageInDays = math.floor((currentTime - tsUpdated) / 86400)
                    if ageInDays < 0 then
                        ageInDays = 0
                    end

                    local calculatedPriority = math.floor(basePriority + underProcBonus + ageInDays)

                    table.insert(eligibleJobs, {
                        jobId = jobId,
                        priority = calculatedPriority
                    })
                end
            end
        end
    end
end

-- Sort by priority descending
table.sort(eligibleJobs, function(a, b)
    return a.priority > b.priority
end)

-- Build result with rank, limited to requested count
local result = {}
for i = 1, math.min(#eligibleJobs, limit) do
    table.insert(result, {
        eligibleJobs[i].jobId,
        eligibleJobs[i].priority,
        i  -- rank
    })
end

return result

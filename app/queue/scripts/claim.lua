-- KEYS[1] = queue:ready, KEYS[2] = queue:processing
-- ARGV[1] = lease duration in ms
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then
  return false
end
local job_id = popped[1]

local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[1]), job_id)
return job_id
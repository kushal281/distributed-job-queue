-- KEYS[1] = queue:processing, KEYS[2] = queue:delayed
-- ARGV[1] = job_id, ARGV[2] = delay_ms
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0   -- someone else (the reaper) already took this job
end
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[2]), ARGV[1])
return 1
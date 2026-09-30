-- KEYS[1] = queue:processing
-- ARGV[1] = job_id, ARGV[2] = lease_ms
if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then
  return 0
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[2]), ARGV[1])
return 1
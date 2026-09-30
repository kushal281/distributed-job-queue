-- KEYS[1] = queue:processing, ARGV[1] = max jobs to reap
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
end
return ids
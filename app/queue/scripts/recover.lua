-- KEYS = ready, delayed, processing
-- ARGV[1] = job_id, ARGV[2] = 'ready' | 'delayed', ARGV[3] = score
for i = 1, 3 do
  if redis.call('ZSCORE', KEYS[i], ARGV[1]) then
    return 0
  end
end
local target = KEYS[1]
if ARGV[2] == 'delayed' then
  target = KEYS[2]
end
redis.call('ZADD', target, ARGV[3], ARGV[1])
return 1
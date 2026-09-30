-- KEYS[1] = queue:delayed, KEYS[2] = queue:ready
-- ARGV[1] = job_id, ARGV[2] = ready score
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
  return 1
end
return 0
-- KEYS[1] = queue:ready
-- ARGV[1] = job_id, ARGV[2] = score
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
-- KEYS[1] = queue:processing, ARGV[1] = job_id
return redis.call('ZREM', KEYS[1], ARGV[1])
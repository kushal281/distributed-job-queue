import time
from pathlib import Path

from app.core.redis_client import redis_client

READY = "queue:ready"
PROCESSING = "queue:processing"
DELAYED = "queue:delayed"

_scripts = Path(__file__).parent / "scripts"
_enqueue = redis_client.register_script((_scripts / "enqueue.lua").read_text())
_claim = redis_client.register_script((_scripts / "claim.lua").read_text())
_ack = redis_client.register_script((_scripts / "ack.lua").read_text())
_requeue = redis_client.register_script((_scripts / "requeue.lua").read_text())
_promote = redis_client.register_script((_scripts / "promote.lua").read_text())
_extend = redis_client.register_script((_scripts / "extend.lua").read_text())
_reap = redis_client.register_script((_scripts / "reap.lua").read_text())
_recover = redis_client.register_script((_scripts / "recover.lua").read_text())

def make_score(priority: int) -> int:
    return priority * 10**13 + int(time.time() * 1000)


async def enqueue(job_id, priority: int) -> None:
    await _enqueue(keys=[READY], args=[str(job_id), make_score(priority)])


async def claim(lease_ms: int = 30_000) -> str | None:
    return await _claim(keys=[READY, PROCESSING], args=[lease_ms])


async def ack(job_id) -> int:
    return await _ack(keys=[PROCESSING], args=[str(job_id)])


async def requeue(job_id, delay_ms: int) -> int:
    return await _requeue(keys=[PROCESSING, DELAYED], args=[str(job_id), delay_ms])


async def due_job_ids(limit: int = 100) -> list[str]:
    sec, usec = await redis_client.time()
    now_ms = sec * 1000 + usec // 1000
    return await redis_client.zrangebyscore(DELAYED, "-inf", now_ms, start=0, num=limit)


async def promote(job_id, priority: int) -> int:
    return await _promote(keys=[DELAYED, READY], args=[str(job_id), make_score(priority)])


async def extend(job_id, lease_ms: int = 30_000) -> int:
    return await _extend(keys=[PROCESSING], args=[str(job_id), lease_ms])


async def reap_expired(limit: int = 100) -> list[str]:
    return await _reap(keys=[PROCESSING], args=[limit])


async def schedule(job_id, delay_ms: int) -> None:
    """Put a job into queue:delayed (used by the reaper, which no longer owns a lease)."""
    sec, usec = await redis_client.time()
    now_ms = sec * 1000 + usec // 1000
    await redis_client.zadd(DELAYED, {str(job_id): now_ms + delay_ms})


async def now_ms() -> int:
    sec, usec = await redis_client.time()
    return sec * 1000 + usec // 1000


async def recover_missing(job_id, target: str, score: int) -> int:
    """1 = job was in no Redis set and has been re-added, 0 = already tracked."""
    return await _recover(keys=[READY, DELAYED, PROCESSING], args=[str(job_id), target, score])
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
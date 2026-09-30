import time
from pathlib import Path

from app.core.redis_client import redis_client

READY = "queue:ready"
PROCESSING = "queue:processing"

_scripts = Path(__file__).parent / "scripts"
_enqueue = redis_client.register_script((_scripts / "enqueue.lua").read_text())
_claim = redis_client.register_script((_scripts / "claim.lua").read_text())
_ack = redis_client.register_script((_scripts / "ack.lua").read_text())


def make_score(priority: int) -> int:
    return priority * 10**13 + int(time.time() * 1000)


async def enqueue(job_id, priority: int) -> None:
    await _enqueue(keys=[READY], args=[str(job_id), make_score(priority)])


async def claim(lease_ms: int = 30_000) -> str | None:
    return await _claim(keys=[READY, PROCESSING], args=[lease_ms])


async def ack(job_id) -> int:
    return await _ack(keys=[PROCESSING], args=[str(job_id)])
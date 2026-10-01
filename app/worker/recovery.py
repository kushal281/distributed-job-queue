import asyncio
from datetime import datetime, timezone

from sqlalchemy import select

from app.core.db import SessionLocal
from app.core.redis_client import redis_client
from app.models.tables import Job
from app.queue import queue
from app.worker.reaper import reclaim

RECOVERY_INTERVAL = 30  # seconds between sweeps
BATCH = 1000


def now():
    return datetime.now(timezone.utc)


async def recover_once() -> int:
    """Compare Postgres (truth) with Redis and repair what's missing. Returns repairs."""
    async with SessionLocal() as s:
        res = await s.execute(
            select(Job.id, Job.status, Job.priority, Job.next_run_at)
            .where(Job.status.in_(["queued", "delayed", "running"]))
            .order_by(Job.created_at)
            .limit(BATCH)
        )
        rows = res.all()

    fixed = 0
    for job_id, status, priority, next_run_at in rows:
        job_id = str(job_id)
        if status == "queued":
            fixed += await queue.recover_missing(job_id, "ready", queue.make_score(priority))
        elif status == "delayed":
            remaining = 0
            if next_run_at:
                remaining = max(0, int((next_run_at - now()).total_seconds() * 1000))
            fixed += await queue.recover_missing(job_id, "delayed", await queue.now_ms() + remaining)
        elif status == "running":
            # healthy running jobs hold a lease; no lease = Redis lost it (or worker died)
            if await redis_client.zscore(queue.PROCESSING, job_id) is None:
                await reclaim(job_id)   # re-reads the row under a lock, so stale rows are safe
                fixed += 1
    return fixed


async def recovery_loop():
    while True:                         # first sweep runs immediately = "on startup"
        try:
            n = await recover_once()
            if n:
                print(f"[recovery] repaired {n} job(s)", flush=True)
        except Exception as e:
            print(f"[recovery] error: {e!r}", flush=True)
        await asyncio.sleep(RECOVERY_INTERVAL)
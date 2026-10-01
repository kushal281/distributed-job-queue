import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.core.backoff import backoff_ms
from app.core.db import SessionLocal
from app.models.tables import Job, JobAttempt
from app.queue import queue

REAP_INTERVAL = 5  # seconds


def now():
    return datetime.now(timezone.utc)


async def reclaim(job_id: str) -> str | None:
    """Fix up one job whose lease is gone. Returns its new status."""
    delay_ms = None
    async with SessionLocal() as s:
        # row lock: a slow worker finishing at the same moment must wait for us
        job = await s.get(Job, uuid.UUID(job_id), with_for_update=True)
        if job is None:
            return None
        if job.status == "running":
            # worker crashed or stalled: close its attempt, then retry or dead
            res = await s.execute(
                select(JobAttempt).where(
                    JobAttempt.job_id == job.id, JobAttempt.outcome.is_(None)
                )
            )
            for a in res.scalars():
                a.outcome, a.finished_at = "lease_expired", now()
                a.error = "lease expired (worker crashed or stalled)"
            job.last_error = "lease expired"
            # attempts was already incremented when the run started,
            # so the crashed run counts as an attempt
            if job.attempts < job.max_attempts:
                delay_ms = backoff_ms(job.attempts)
                job.status = "delayed"
                job.next_run_at = now() + timedelta(milliseconds=delay_ms)
            else:
                job.status = "dead"
                job.finished_at = now()
        elif job.status == "delayed":
            # worker saved the retry in Postgres but died before requeue
            delay_ms = max(0, int((job.next_run_at - now()).total_seconds() * 1000))
        # succeeded / dead: already finished, only the ack was missing
        await s.commit()
        status = job.status
    if delay_ms is not None:
        await queue.schedule(job_id, delay_ms)
    return status


async def reap_once() -> int:
    ids = await queue.reap_expired()
    for job_id in ids:
        status = await reclaim(job_id)
        print(f"[reaper] reclaimed {job_id} -> {status}", flush=True)
    return len(ids)


async def reaper_loop():
    while True:
        try:
            await reap_once()
        except Exception as e:
            print(f"[reaper] error: {e!r}", flush=True)
        await asyncio.sleep(REAP_INTERVAL)
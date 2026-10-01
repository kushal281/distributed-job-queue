import asyncio
import os
import socket
import uuid
import time
from datetime import datetime, timedelta, timezone

from app.core.db import SessionLocal
from app.core.backoff import backoff_ms
from app.core.redis_client import redis_client
from app.models.tables import Job, JobAttempt
from app.queue.queue import ack, claim, requeue, extend
from app.worker.scheduler import scheduler_loop
from app.worker.handlers import HANDLERS
from app.worker.reaper import reaper_loop
from app.worker.recovery import recovery_loop

WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
POLL_INTERVAL = 0.5
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))
HEARTBEAT_INTERVAL = 3      # seconds between beats
HEARTBEAT_TTL = 10          # worker key expires after 10s without a beat
LEASE_MS = 30_000
STALE = -1   # run_job returns this when the result was discarded (fencing)

active: set[str] = set()    # job ids running on THIS worker


def now():
    return datetime.now(timezone.utc)


async def run_job(job_id: str):
    jid = uuid.UUID(job_id)

    # 1. mark running + record the attempt
    async with SessionLocal() as s:
        job = await s.get(Job, jid)
        if job is None:
            return
        job.status = "running"
        job.attempts += 1
        job.started_at = now()
        attempt = JobAttempt(job_id=jid, worker_id=WORKER_ID)
        s.add(attempt)
        await s.commit()
        job_type, payload, attempt_id, my_attempt = (
            job.type, job.payload, attempt.id, job.attempts
        )

    # 2. run the handler
    try:
        result = await HANDLERS[job_type](payload)
        outcome, error = "succeeded", None
    except Exception as e:
        result, outcome, error = None, "failed", repr(e)

    # 3. write the outcome
    retry_delay_ms = None
    async with SessionLocal() as s:
        job = await s.get(Job, jid, with_for_update=True)
        attempt = await s.get(JobAttempt, attempt_id)
        if job.attempts != my_attempt or job.status != "running":
            # the reaper reclaimed this job (or another worker re-ran it): drop our result
            attempt.outcome = "superseded"
            attempt.finished_at = now()
            await s.commit()
            return STALE
        attempt.outcome = outcome
        attempt.error = error
        attempt.finished_at = now()
        job.last_error = error
        if outcome == "succeeded":
            job.status = "succeeded"
            job.result = result
            job.finished_at = now()
        elif job.attempts < job.max_attempts:
            retry_delay_ms = backoff_ms(job.attempts)
            job.status = "delayed"
            job.next_run_at = now() + timedelta(milliseconds=retry_delay_ms)
        else:
            job.status = "dead"         
            job.finished_at = now()
        await s.commit()
    return retry_delay_ms


async def heartbeat_loop():
    while True:
        try:
            await redis_client.set(f"worker:{WORKER_ID}", str(len(active)), ex=HEARTBEAT_TTL)
            await redis_client.zadd("workers:seen", {WORKER_ID: int(time.time() * 1000)})
            for job_id in list(active):
                if await extend(job_id, LEASE_MS) == 0 and job_id in active:
                    print(f"[{WORKER_ID}] LOST lease on {job_id}", flush=True)
        except Exception as e:
            print(f"[{WORKER_ID}] heartbeat error: {e!r}", flush=True)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def handle(job_id: str, sem: asyncio.Semaphore):
    active.add(job_id)
    try:
        delay_ms = await run_job(job_id)
        if delay_ms == STALE:
            print(f"[{WORKER_ID}] STALE, result discarded for {job_id}", flush=True)
        else:
            if delay_ms is None:
                await ack(job_id)
            else:
                await requeue(job_id, delay_ms)
            print(f"[{WORKER_ID}] done {job_id}", flush=True)
    except Exception as e:
        # job stays in queue:processing; the reaper will reclaim it
        print(f"[{WORKER_ID}] error on {job_id}: {e!r}", flush=True)
    finally:
        active.discard(job_id)
        sem.release()


async def main():
    print(f"[{WORKER_ID}] started, concurrency={CONCURRENCY}", flush=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    scheduler = asyncio.create_task(scheduler_loop())   # keep a reference
    heartbeat = asyncio.create_task(heartbeat_loop())   # keep a reference
    reaper = asyncio.create_task(reaper_loop())   # keep a reference
    recovery = asyncio.create_task(recovery_loop())   # keep a reference
        
    tasks: set[asyncio.Task] = set()
    while True:
        await sem.acquire()                 # wait for a free slot BEFORE claiming
        job_id = await claim()
        if job_id is None:
            sem.release()
            await asyncio.sleep(POLL_INTERVAL)
            continue
        print(f"[{WORKER_ID}] claimed {job_id}", flush=True)
        task = asyncio.create_task(handle(job_id, sem))
        tasks.add(task)                     # keep a reference so it isn't garbage collected
        task.add_done_callback(tasks.discard)

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

from app.core.db import SessionLocal
from app.core.backoff import backoff_ms
from app.models.tables import Job, JobAttempt
from app.queue.queue import ack, claim, requeue
from app.worker.scheduler import scheduler_loop
from app.worker.handlers import HANDLERS


WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
POLL_INTERVAL = 0.5
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))


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
        job_type, payload, attempt_id = job.type, job.payload, attempt.id

    # 2. run the handler
    try:
        result = await HANDLERS[job_type](payload)
        outcome, error = "succeeded", None
    except Exception as e:
        result, outcome, error = None, "failed", repr(e)

    # 3. write the outcome
    retry_delay_ms = None
    async with SessionLocal() as s:
        job = await s.get(Job, jid)
        attempt = await s.get(JobAttempt, attempt_id)
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
            job.status = "dead"          # Step 7 adds the DLQ endpoints
            job.finished_at = now()
        await s.commit()
    return retry_delay_ms


async def handle(job_id: str, sem: asyncio.Semaphore):
    try:
        delay_ms = await run_job(job_id)
        if delay_ms is None:
            await ack(job_id)
        else:
            await requeue(job_id, delay_ms)
        print(f"[{WORKER_ID}] done {job_id}", flush=True)
    except Exception as e:
        # job stays in queue:processing; the reaper (Step 8) will reclaim it
        print(f"[{WORKER_ID}] error on {job_id}: {e!r}", flush=True)
    finally:
        sem.release()


async def main():
    print(f"[{WORKER_ID}] started, concurrency={CONCURRENCY}", flush=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    scheduler = asyncio.create_task(scheduler_loop())   # keep a reference
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
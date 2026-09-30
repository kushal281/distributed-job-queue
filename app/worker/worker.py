import asyncio
import os
import socket
import uuid
from datetime import datetime, timezone

from app.core.db import SessionLocal
from app.models.tables import Job, JobAttempt
from app.queue.queue import ack, claim
from app.worker.handlers import HANDLERS

WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
POLL_INTERVAL = 0.5


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
    async with SessionLocal() as s:
        job = await s.get(Job, jid)
        attempt = await s.get(JobAttempt, attempt_id)
        job.status = outcome
        job.result = result
        job.last_error = error
        job.finished_at = now()
        attempt.outcome = outcome
        attempt.error = error
        attempt.finished_at = now()
        await s.commit()


async def main():
    print(f"[{WORKER_ID}] started", flush=True)
    while True:
        job_id = await claim()
        if job_id is None:
            await asyncio.sleep(POLL_INTERVAL)
            continue
        print(f"[{WORKER_ID}] claimed {job_id}", flush=True)
        await run_job(job_id)
        await ack(job_id)          # only after Postgres is updated
        print(f"[{WORKER_ID}] done {job_id}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
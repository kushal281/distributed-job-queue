import asyncio
import uuid

from sqlalchemy import select, update

from app.core.db import SessionLocal
from app.models.tables import Job
from app.queue.queue import due_job_ids, promote


async def scheduler_loop(interval: float = 1.0):
    while True:
        try:
            for jid in await due_job_ids():
                job_id = uuid.UUID(jid)
                async with SessionLocal() as s:
                    row = (await s.execute(select(Job.priority).where(Job.id == job_id))).first()
                    if row is None:
                        continue
                    # conditional update: only flips delayed -> queued, never clobbers 'running'
                    await s.execute(
                        update(Job).where(Job.id == job_id, Job.status == "delayed").values(status="queued")
                    )
                    await s.commit()
                await promote(job_id, row[0])
        except Exception as e:
            print(f"[scheduler] error: {e!r}", flush=True)
        await asyncio.sleep(interval)
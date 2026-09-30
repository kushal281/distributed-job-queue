import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.models.tables import Job
from app.queue.queue import enqueue

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobCreate(BaseModel):
    type: str
    payload: dict[str, Any] = {}
    priority: int = Field(5, ge=0, le=9)        # 0 = highest
    max_attempts: int = Field(3, ge=1, le=20)


class JobOut(BaseModel):
    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: int
    status: str
    attempts: int
    max_attempts: int
    result: dict[str, Any] | None
    last_error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


@router.post("", response_model=JobOut, status_code=201)
async def create_job(body: JobCreate, session: AsyncSession = Depends(get_session)):
    job = Job(**body.model_dump())
    session.add(job)
    await session.commit()
    await session.refresh(job)
    await enqueue(job.id, job.priority)
    return job


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    job = await session.get(Job, job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job


@router.post("/{job_id}/retry", response_model=JobOut)
async def retry_job(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    # One atomic conditional UPDATE: only a dead job can flip to queued,
    # so a double-click or two clients can't enqueue it twice.
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == "dead")
        .values(status="queued", attempts=0, last_error=None, finished_at=None, next_run_at=None)
        .returning(Job.id, Job.priority)
    )
    row = res.first()
    await session.commit()
    if row is None:
        if await session.get(Job, job_id) is None:
            raise HTTPException(404, "job not found")
        raise HTTPException(409, "only dead jobs can be retried")
    await enqueue(row.id, row.priority)
    return await session.get(Job, job_id)


dlq_router = APIRouter(tags=["dlq"])


@dlq_router.get("/dlq", response_model=list[JobOut])
async def list_dlq(limit: int = Query(50, ge=1, le=200), session: AsyncSession = Depends(get_session)):
    rows = await session.execute(
        select(Job).where(Job.status == "dead").order_by(Job.finished_at.desc()).limit(limit)
    )
    return rows.scalars().all()
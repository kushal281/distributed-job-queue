import asyncio
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.redis_client import redis_client
from app.models.tables import Job
from app.queue.queue import enqueue

IDEM_TTL = 86400  # seconds

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


async def _find_by_key(session: AsyncSession, key: str) -> Job | None:
    res = await session.execute(select(Job).where(Job.idempotency_key == key))
    return res.scalar_one_or_none()


def _same_request(existing: Job, body: JobCreate) -> None:
    # same key + different request is a client bug, don't silently return the wrong job
    if existing.type != body.type or existing.payload != body.payload:
        raise HTTPException(422, "Idempotency-Key was already used with a different request")


@router.post("", response_model=JobOut, status_code=201)
async def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, max_length=200),
    session: AsyncSession = Depends(get_session),
):
    job_id = uuid.uuid4()

    if idempotency_key:
        # Gate 1 (Redis): only one request wins SET NX
        won = await redis_client.set(f"idem:{idempotency_key}", str(job_id), nx=True, ex=IDEM_TTL)
        if not won:
            # duplicate: the winner may still be committing, so poll Postgres for up to 2s
            for _ in range(20):
                existing = await _find_by_key(session, idempotency_key)
                if existing:
                    _same_request(existing, body)
                    response.status_code = 200
                    return existing
                await asyncio.sleep(0.1)
            # no row after 2s: stale key (winner died). Fall through and insert;
            # the unique index below still prevents a double insert.

    job = Job(**body.model_dump(), id=job_id, idempotency_key=idempotency_key or None)
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        # Gate 2 (Postgres unique index): covers Redis key expired / Redis wiped
        await session.rollback()
        existing = await _find_by_key(session, idempotency_key) if idempotency_key else None
        if existing is None:
            raise
        _same_request(existing, body)
        response.status_code = 200
        return existing
    except Exception:
        await session.rollback()
        if idempotency_key:  # let the client retry with the same key
            await redis_client.delete(f"idem:{idempotency_key}")
        raise

    await session.refresh(job)
    await enqueue(job.id, job.priority)
    return job


@router.get("", response_model=list[JobOut])
async def list_jobs(
    status: str | None = None,
    type: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    q = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status:
        q = q.where(Job.status == status)
    if type:
        q = q.where(Job.type == type)
    res = await session.execute(q)
    return res.scalars().all()


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
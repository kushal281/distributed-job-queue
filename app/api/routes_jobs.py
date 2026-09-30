import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.models.tables import Job

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
    return job


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    job = await session.get(Job, job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job
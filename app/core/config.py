from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://jobs:jobs@localhost:5432/jobs"
    redis_url: str = "redis://localhost:6379/0"

    @field_validator("database_url")
    @classmethod
    def use_asyncpg_driver(cls, v: str) -> str:
        # hosted Postgres gives postgresql:// or postgres://; the async engine needs asyncpg
        for prefix in ("postgresql://", "postgres://"):
            if v.startswith(prefix):
                return "postgresql+asyncpg://" + v[len(prefix):]
        return v


settings = Settings()
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://jobs:jobs@localhost:5432/jobs"
    redis_url: str = "redis://localhost:6379/0"


settings = Settings()
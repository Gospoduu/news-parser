from pydantic import BaseModel, Field


class SourceCreate(BaseModel):
    name: str
    rss_url: str
    site_url: str | None = None
    enabled: bool = True
    poll_interval_seconds: int = Field(default=300, gt=0)


class Source(SourceCreate):
    id: int

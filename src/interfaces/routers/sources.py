from fastapi import APIRouter, HTTPException, Request, status

from src.infra.schemas.sources import Source, SourceCreate
from src.services.sources import SourceService


router = APIRouter(
    prefix="/sources",
    tags=["Sources"],
)

service = SourceService()


@router.post(
    "",
    response_model=Source,
    status_code=status.HTTP_201_CREATED,
)
async def create_source(data: SourceCreate, request: Request) -> Source:
    source = await service.create_source(data)
    await request.app.state.background_worker.add_source(source)
    return source


@router.patch("/{source_id}/enable", response_model=Source)
async def enable_source(source_id: int, request: Request) -> Source:
    source = await service.enable_source(source_id)

    if source is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Источник не найден",
        )

    await request.app.state.background_worker.add_source(source)
    return source


@router.patch("/{source_id}/disable", response_model=Source)
async def disable_source(source_id: int, request: Request) -> Source:
    source = await service.disable_source(source_id)

    if source is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Источник не найден",
        )

    await request.app.state.background_worker.remove_source(source_id)
    return source


@router.get("", response_model=list[Source])
async def get_sources() -> list[Source]:
    return await service.get_sources()

import asyncio
import os

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas
from app.scraper import get_epg_now_next

EPG_FILE = os.getenv("EPG_FILE", "./data/epg.xml")

router = APIRouter(prefix="/api/groups", tags=["groups"])


@router.get("/", response_model=list[schemas.GroupOut])
async def list_groups(db: AsyncSession = Depends(get_db)):
    return await crud.get_groups(db)


@router.post("/", response_model=schemas.GroupOut, status_code=status.HTTP_201_CREATED)
async def create_group(data: schemas.GroupCreate, db: AsyncSession = Depends(get_db)):
    if await crud.get_group_by_name(db, data.name):
        raise HTTPException(status_code=409, detail="Group name already exists")
    return await crud.create_group(db, data)


@router.get("/{group_id}/channels")
async def get_group_channels(group_id: int, db: AsyncSession = Depends(get_db)):
    return await crud.get_group_channel_ids_ordered(db, group_id)


@router.put("/{group_id}/channel-order", status_code=status.HTTP_204_NO_CONTENT)
async def update_channel_order(
    group_id: int,
    data: schemas.GroupChannelOrderUpdate,
    db: AsyncSession = Depends(get_db),
):
    await crud.update_group_channel_order(db, group_id, data.channel_ids)


@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(group_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_group(db, group_id)


@router.get("/{group_id}/guide")
async def group_guide(group_id: int, db: AsyncSession = Depends(get_db)):
    channels = await crud.get_group_channels_with_active_sources(db, group_id)
    if not channels:
        return []
    tvg_ids = {ch.tvg_id for ch in channels}
    epg = await asyncio.to_thread(get_epg_now_next, tvg_ids, EPG_FILE)
    result = []
    for ch in channels:
        active = sum(1 for s in ch.sources if s.active)
        guide  = epg.get(ch.tvg_id, {})
        result.append({
            "id":            ch.id,
            "tvg_id":        ch.tvg_id,
            "name":          ch.name,
            "logo":          f"/static/logos/{ch.custom_logo}" if ch.custom_logo else ch.logo,
            "active_sources": active,
            "current":       guide.get("current"),
            "next":          guide.get("next"),
        })
    return result

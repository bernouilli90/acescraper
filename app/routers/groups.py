from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas

router = APIRouter(prefix="/api/groups", tags=["groups"])


@router.get("/", response_model=list[schemas.GroupOut])
async def list_groups(db: AsyncSession = Depends(get_db)):
    return await crud.get_groups(db)


@router.post("/", response_model=schemas.GroupOut, status_code=status.HTTP_201_CREATED)
async def create_group(data: schemas.GroupCreate, db: AsyncSession = Depends(get_db)):
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

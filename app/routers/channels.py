import glob
import os

import aiofiles
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app import crud, schemas

router = APIRouter(prefix="/api/channels", tags=["channels"])

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
LOGOS_DIR = os.getenv("LOGOS_DIR", os.path.join(_ROOT, "static", "logos"))

_ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif", "image/svg+xml"}
_EXT_MAP = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/svg+xml": ".svg",
}


@router.get("/", response_model=list[schemas.ChannelOut])
async def list_channels(skip: int = 0, limit: int = 10000, db: AsyncSession = Depends(get_db)):
    return await crud.get_channels(db, skip=skip, limit=limit)


@router.post("/", response_model=schemas.ChannelOut, status_code=status.HTTP_201_CREATED)
async def create_channel(data: schemas.ChannelCreate, db: AsyncSession = Depends(get_db)):
    existing = await crud.get_channel_by_tvg_id(db, data.tvg_id)
    if existing:
        raise HTTPException(status_code=409, detail="tvg_id already exists")
    return await crud.create_channel(db, data)


@router.post("/{channel_id}/logo", response_model=schemas.ChannelOut)
async def upload_logo(
    channel_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")

    ct = (file.content_type or "").split(";")[0].strip()
    if ct not in _ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail=f"Tipo no soportado: {ct}")

    ext = _EXT_MAP.get(ct, ".jpg")
    filename = f"channel_{channel_id}{ext}"

    os.makedirs(LOGOS_DIR, exist_ok=True)
    for old in glob.glob(os.path.join(LOGOS_DIR, f"channel_{channel_id}.*")):
        os.remove(old)

    async with aiofiles.open(os.path.join(LOGOS_DIR, filename), "wb") as f:
        await f.write(await file.read())

    await crud.set_channel_logo(db, channel_id, filename)
    return await crud.get_channel(db, channel_id)


@router.delete("/{channel_id}/logo", status_code=status.HTTP_204_NO_CONTENT)
async def delete_logo(channel_id: int, db: AsyncSession = Depends(get_db)):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")

    if ch.custom_logo:
        path = os.path.join(LOGOS_DIR, ch.custom_logo)
        if os.path.exists(path):
            os.remove(path)

    await crud.set_channel_logo(db, channel_id, None)


@router.get("/{channel_id}", response_model=schemas.ChannelOut)
async def get_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    ch = await crud.get_channel(db, channel_id)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    return ch


@router.patch("/{channel_id}", response_model=schemas.ChannelOut)
async def update_channel(channel_id: int, data: schemas.ChannelUpdate, db: AsyncSession = Depends(get_db)):
    ch = await crud.update_channel(db, channel_id, data)
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    return ch


@router.delete("/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_channel(db, channel_id)

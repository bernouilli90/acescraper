from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas

router = APIRouter(prefix="/api/export-profiles", tags=["export-profiles"])


@router.get("/", response_model=list[schemas.ExportProfileOut])
async def list_profiles(db: AsyncSession = Depends(get_db)):
    return await crud.list_export_profiles(db)


@router.post("/", response_model=schemas.ExportProfileOut, status_code=201)
async def create_profile(data: schemas.ExportProfileIn, db: AsyncSession = Depends(get_db)):
    return await crud.create_export_profile(db, data)


@router.patch("/{profile_id}", response_model=schemas.ExportProfileOut)
async def update_profile(
    profile_id: int,
    data: schemas.ExportProfileIn,
    db: AsyncSession = Depends(get_db),
):
    obj = await crud.update_export_profile(db, profile_id, data)
    if not obj:
        raise HTTPException(404, "Perfil no encontrado")
    return obj


@router.delete("/{profile_id}", status_code=204)
async def delete_profile(profile_id: int, db: AsyncSession = Depends(get_db)):
    obj = await crud.delete_export_profile(db, profile_id)
    if not obj:
        raise HTTPException(404, "Perfil no encontrado")

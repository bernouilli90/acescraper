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


@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(group_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_group(db, group_id)

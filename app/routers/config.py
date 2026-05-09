from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("/", response_model=schemas.ConfigOut)
async def get_config(db: AsyncSession = Depends(get_db)):
    cfg = await crud.get_config(db)
    return schemas.ConfigOut(
        acexy_ip=cfg.get("acexy_ip", "127.0.0.1"),
        acexy_port=int(cfg.get("acexy_port", "6878")),
    )


@router.put("/", response_model=schemas.ConfigOut)
async def update_config(data: schemas.ConfigUpdate, db: AsyncSession = Depends(get_db)):
    await crud.set_config(db, "acexy_ip", data.acexy_ip)
    await crud.set_config(db, "acexy_port", str(data.acexy_port))
    return schemas.ConfigOut(acexy_ip=data.acexy_ip, acexy_port=data.acexy_port)

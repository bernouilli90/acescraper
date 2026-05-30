import asyncio

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas, scraper

router = APIRouter(prefix="/api/xmltv", tags=["xmltv"])


@router.get("/", response_model=list[schemas.XmltvUrlOut])
async def list_xmltv(db: AsyncSession = Depends(get_db)):
    return await crud.get_xmltv_urls(db)


@router.post("/", response_model=schemas.XmltvUrlOut, status_code=status.HTTP_201_CREATED)
async def create_xmltv(data: schemas.XmltvUrlCreate, db: AsyncSession = Depends(get_db)):
    return await crud.create_xmltv_url(db, data)


@router.delete("/{xmltv_id}")
async def delete_xmltv(xmltv_id: int, db: AsyncSession = Depends(get_db)):
    result = await crud.delete_xmltv_url(db, xmltv_id)
    if result is None:
        raise HTTPException(status_code=404, detail="XMLTV no encontrada")
    return result


@router.post("/{xmltv_id}/refresh", response_model=schemas.XmltvImportResult)
async def refresh_xmltv(xmltv_id: int, db: AsyncSession = Depends(get_db)):
    xmltv = await crud.get_xmltv_url(db, xmltv_id)
    if not xmltv:
        raise HTTPException(status_code=404, detail="XMLTV no encontrada")
    try:
        channels = await scraper.fetch_xmltv(xmltv.url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al descargar: {e}")
    return await crud.import_xmltv_channels(db, channels, xmltv_id)


@router.post("/import-file", response_model=schemas.XmltvImportResult)
async def import_xmltv_file(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    channels = await asyncio.to_thread(scraper.parse_xmltv, file.file)
    result = await crud.bulk_import_channels(db, channels)
    return schemas.XmltvImportResult(**result)

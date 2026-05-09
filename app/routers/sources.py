from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas, scraper, models

router = APIRouter(prefix="/api/sources", tags=["sources"])


@router.get("/", response_model=list[schemas.SourceOut])
async def list_sources(
    skip: int = 0,
    limit: int = 1000,
    unmapped_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    return await crud.get_sources(db, skip=skip, limit=limit, unmapped_only=unmapped_only)


@router.post("/", response_model=schemas.SourceOut, status_code=status.HTTP_201_CREATED)
async def create_source(data: schemas.SourceCreate, db: AsyncSession = Depends(get_db)):
    existing = await crud.get_source_by_hash(db, data.ace_hash)
    if existing:
        raise HTTPException(status_code=409, detail="Hash already exists")
    return await crud.create_source(db, data)


def _result(r: dict, total: int) -> schemas.BulkImportResult:
    return schemas.BulkImportResult(
        new_hashes=r["new"],
        duplicates=r["duplicates"],
        total_found=total,
        auto_mapped=r["mapped"],
    )


@router.post("/import/text", response_model=schemas.BulkImportResult)
async def import_from_text(
    text: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    entries = scraper.parse_m3u(text)
    result = await crud.bulk_create_sources(db, entries)
    return _result(result, len(entries))


@router.post("/import/file", response_model=schemas.BulkImportResult)
async def import_from_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    content = await file.read()
    entries = scraper.parse_m3u(content.decode("utf-8", errors="ignore"))
    result = await crud.bulk_create_sources(db, entries)
    return _result(result, len(entries))


@router.post("/import/url", response_model=schemas.BulkImportResult)
async def import_from_url(
    url: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    try:
        entries = await scraper.fetch_and_parse(url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")
    result = await crud.bulk_create_sources(db, entries)
    return _result(result, len(entries))


@router.patch("/bulk-assign", status_code=status.HTTP_200_OK)
async def bulk_assign_sources(data: schemas.BulkAssignRequest, db: AsyncSession = Depends(get_db)):
    from sqlalchemy import update
    await db.execute(
        update(models.Source)
        .where(models.Source.id.in_(data.ids))
        .values(channel_id=data.channel_id)
    )
    await db.commit()
    return {"updated": len(data.ids)}


@router.patch("/{source_id}", response_model=schemas.SourceOut)
async def update_source(source_id: int, data: schemas.SourceUpdate, db: AsyncSession = Depends(get_db)):
    src = await crud.update_source(db, source_id, data)
    if not src:
        raise HTTPException(status_code=404, detail="Source not found")
    return src


@router.delete("/bulk", status_code=status.HTTP_200_OK)
async def bulk_delete_sources(ids: list[int], db: AsyncSession = Depends(get_db)):
    deleted = await crud.bulk_delete_sources(db, ids)
    return {"deleted": deleted}


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_source(source_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_source(db, source_id)

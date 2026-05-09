from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app import crud, schemas, scraper

router = APIRouter(prefix="/api/feeds", tags=["feeds"])


@router.get("/", response_model=list[schemas.FeedUrlOut])
async def list_feeds(db: AsyncSession = Depends(get_db)):
    return await crud.get_feed_urls(db)


@router.post("/", response_model=schemas.FeedUrlOut, status_code=status.HTTP_201_CREATED)
async def create_feed(data: schemas.FeedUrlCreate, db: AsyncSession = Depends(get_db)):
    return await crud.create_feed_url(db, data)


@router.delete("/{feed_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_feed(feed_id: int, db: AsyncSession = Depends(get_db)):
    await crud.delete_feed_url(db, feed_id)


@router.post("/{feed_id}/refresh", response_model=schemas.BulkImportResult)
async def refresh_feed(feed_id: int, db: AsyncSession = Depends(get_db)):
    feed = await crud.get_feed_url(db, feed_id)
    if not feed:
        raise HTTPException(status_code=404, detail="Feed not found")
    try:
        entries = await scraper.fetch_and_parse(feed.url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Fetch failed: {e}")
    result = await crud.bulk_create_sources(db, entries, feed_url_id=feed_id)
    await crud.update_feed_url_stats(db, feed_id, len(entries))
    return schemas.BulkImportResult(
        new_hashes=result["new"],
        duplicates=result["duplicates"],
        total_found=len(entries),
        auto_mapped=result["mapped"],
    )


@router.post("/refresh-all")
async def refresh_all(db: AsyncSession = Depends(get_db)):
    await scraper.refresh_all_feeds(db)
    await scraper.refresh_all_xmltv(db)
    return {"detail": "All active feeds refreshed"}



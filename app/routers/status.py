from fastapi import APIRouter
from app.version import APP_VERSION

router = APIRouter(prefix="/api/status", tags=["status"])


@router.get("")
async def get_status():
    from app.scraper import test_progress, feed_progress
    from app.routers.watchdog import _state as wd_state

    return {
        "version": APP_VERSION,
        "stream_test": {
            "running":       test_progress["running"],
            "current":       test_progress["current"],
            "total":         test_progress["total"],
            "current_hash":  test_progress["current_hash"],
            "current_label": test_progress["current_label"],
            "recent":        test_progress["recent"],
        },
        "feeds": {
            "running":     feed_progress["running"],
            "current":     feed_progress["current"],
            "total":       feed_progress["total"],
            "current_url": feed_progress["current_url"],
        },
        "watchdog": wd_state,
    }

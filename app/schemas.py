from pydantic import BaseModel, HttpUrl, field_validator
from typing import Optional
from datetime import datetime


# --- Group ---
class GroupBase(BaseModel):
    name: str

class GroupCreate(GroupBase):
    pass

class GroupOut(GroupBase):
    id: int
    model_config = {"from_attributes": True}


# --- Channel ---
class ChannelBase(BaseModel):
    tvg_id: str
    name: str
    logo: Optional[str] = None

class ChannelCreate(ChannelBase):
    group_ids: list[int] = []

class ChannelUpdate(BaseModel):
    name: Optional[str] = None
    logo: Optional[str] = None
    group_ids: Optional[list[int]] = None

class ChannelOut(ChannelBase):
    id: int
    custom_logo: Optional[str] = None
    groups: list[GroupOut] = []
    source_count: int = 0
    xmltv_ids: list[int] = []
    model_config = {"from_attributes": True}


# --- Source ---
class SourceBase(BaseModel):
    ace_hash: str
    label: Optional[str] = None
    active: bool = True

    @field_validator("ace_hash")
    @classmethod
    def validate_hash(cls, v: str) -> str:
        v = v.strip().lower()
        if len(v) != 40 or not v.isalnum():
            raise ValueError("ace_hash must be a 40-character alphanumeric string")
        return v

class SourceCreate(SourceBase):
    channel_id: Optional[int] = None

class SourceUpdate(BaseModel):
    channel_id: Optional[int] = None
    label: Optional[str] = None
    active: Optional[bool] = None

class SourceOut(SourceBase):
    id: int
    channel_id: Optional[int] = None
    feed_url_id: Optional[int] = None
    created_at: Optional[datetime] = None
    test_status: str = "untested"
    test_last_run: Optional[datetime] = None
    fail_since: Optional[datetime] = None
    model_config = {"from_attributes": True}



# --- FeedUrl ---
class FeedUrlBase(BaseModel):
    url: str
    label: Optional[str] = None
    active: bool = True

class FeedUrlCreate(FeedUrlBase):
    pass

class FeedUrlOut(FeedUrlBase):
    id: int
    last_fetched: Optional[datetime] = None
    last_count: int = 0
    model_config = {"from_attributes": True}


# --- XmltvUrl ---
class XmltvUrlBase(BaseModel):
    url: str
    label: Optional[str] = None

class XmltvUrlCreate(XmltvUrlBase):
    pass

class XmltvUrlOut(XmltvUrlBase):
    id: int
    last_fetched: Optional[datetime] = None
    channel_count: int = 0
    created_at: Optional[datetime] = None
    model_config = {"from_attributes": True}

class XmltvImportResult(BaseModel):
    created: int
    updated: int
    total: int
    deleted_channels: Optional[int] = None



# --- Config ---
class ConfigUpdate(BaseModel):
    acexy_ip: str = "127.0.0.1"
    acexy_port: int = 6878

class ConfigOut(BaseModel):
    acexy_ip: str
    acexy_port: int


# --- Group channel order ---
class GroupChannelOrderUpdate(BaseModel):
    channel_ids: list[int]


# --- Bulk assign ---
class BulkAssignRequest(BaseModel):
    ids: list[int]
    channel_id: Optional[int] = None


# --- Bulk import ---
class BulkImportResult(BaseModel):
    new_hashes: int
    duplicates: int
    total_found: int
    auto_mapped: int = 0


# --- Scheduler ---
class SchedulerConfig(BaseModel):
    enabled: bool = True
    interval_minutes: int = 360  # 6 hours default

class SchedulerStatus(SchedulerConfig):
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    running: bool = False


# --- EPG ---
class EpgConfig(BaseModel):
    enabled: bool = False
    interval_hours: int = 24
    days: int = 7

class EpgStatus(EpgConfig):
    next_run: Optional[datetime] = None
    last_generated: Optional[datetime] = None
    channel_count: int = 0
    programme_count: int = 0
    file_size_kb: float = 0.0


# --- Stream test ---
class StreamTestConfig(BaseModel):
    enabled: bool = False
    interval_fail_minutes: int = 60
    interval_ok_minutes: int = 360
    concurrency: int = 5
    restart_before_test: bool = False
    restart_after_test: bool = False
    restart_containers: str = ""   # comma-separated container names
    restart_wait_seconds: int = 15
    auto_deactivate_enabled: bool = False
    auto_deactivate_hours: int = 48

class StreamTestStatus(StreamTestConfig):
    next_run_fail: Optional[datetime] = None
    next_run_ok: Optional[datetime] = None
    last_run: Optional[datetime] = None

class StreamTestResult(BaseModel):
    source_id: int
    status: str  # "ok" | "fail"


# --- Export Profile ---
class ExportProfileIn(BaseModel):
    name: str
    url_type: str = "native"
    name_template: str = "{canal}"

class ExportProfileOut(ExportProfileIn):
    id: int
    created_at: Optional[datetime] = None
    model_config = {"from_attributes": True}


# --- Watchdog ---
class WatchdogConfig(BaseModel):
    enabled: bool = False
    interval_minutes: int = 5
    auto_restart: bool = True
    acexy_container: str = "acexy"
    acestream_container: str = "acestream"

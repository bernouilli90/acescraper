from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Table, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base

# N:M association between channels and groups
channel_group = Table(
    "channel_group",
    Base.metadata,
    Column("channel_id", Integer, ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True),
    Column("group_id", Integer, ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True),
    Column("position", Integer, default=0),
)

# N:M association between channels and xmltv sources
channel_xmltv = Table(
    "channel_xmltv",
    Base.metadata,
    Column("channel_id", Integer, ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True),
    Column("xmltv_id", Integer, ForeignKey("xmltv_urls.id", ondelete="CASCADE"), primary_key=True),
)


class Channel(Base):
    __tablename__ = "channels"

    id = Column(Integer, primary_key=True, index=True)
    tvg_id = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    logo = Column(Text, nullable=True)
    custom_logo = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    sources = relationship("Source", back_populates="channel", cascade="all, delete-orphan")
    groups = relationship("Group", secondary=channel_group, back_populates="channels")
    xmltv_urls = relationship("XmltvUrl", secondary=channel_xmltv, back_populates="channels")


class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    ace_hash = Column(String(40), unique=True, index=True, nullable=False)
    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="SET NULL"), nullable=True)
    label = Column(String(255), nullable=True)
    active = Column(Boolean, default=True)
    feed_url_id = Column(Integer, ForeignKey("feed_urls.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    test_status = Column(String(10), nullable=False, default="untested")
    test_last_run = Column(DateTime(timezone=True), nullable=True)
    fail_since = Column(DateTime(timezone=True), nullable=True)

    channel = relationship("Channel", back_populates="sources")
    feed_url = relationship("FeedUrl", back_populates="sources")


class Group(Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False)

    channels = relationship("Channel", secondary=channel_group, back_populates="groups")


class FeedUrl(Base):
    __tablename__ = "feed_urls"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(Text, nullable=False)
    label = Column(String(255), nullable=True)
    last_fetched = Column(DateTime(timezone=True), nullable=True)
    last_count = Column(Integer, default=0)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    sources = relationship("Source", back_populates="feed_url")


class XmltvUrl(Base):
    __tablename__ = "xmltv_urls"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(Text, nullable=False)
    label = Column(String(255), nullable=True)
    last_fetched = Column(DateTime(timezone=True), nullable=True)
    channel_count = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    channels = relationship("Channel", secondary=channel_xmltv, back_populates="xmltv_urls")


class AppConfig(Base):
    __tablename__ = "app_config"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=True)

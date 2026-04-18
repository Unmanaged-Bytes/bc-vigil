from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


ALGORITHMS = ("crc32", "sha256", "xxh3", "xxh128")

SCAN_PENDING = "pending"
SCAN_RUNNING = "running"
SCAN_OK = "ok"
SCAN_DRIFT = "drift"
SCAN_FAILED = "failed"
SCAN_CANCELLED = "cancelled"

EVENT_ADDED = "added"
EVENT_REMOVED = "removed"
EVENT_MODIFIED = "modified"


class Target(Base):
    __tablename__ = "targets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    path: Mapped[str] = mapped_column(Text)
    algorithm: Mapped[str] = mapped_column(String(16), default="sha256")
    threads: Mapped[str] = mapped_column(String(8), default="auto")
    includes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    excludes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    baseline_scan_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("scans.id", use_alter=True, ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    schedules: Mapped[list["Schedule"]] = relationship(
        back_populates="target", cascade="all, delete-orphan",
        foreign_keys="Schedule.target_id",
    )
    scans: Mapped[list["Scan"]] = relationship(
        back_populates="target", cascade="all, delete-orphan",
        foreign_keys="Scan.target_id",
        order_by="Scan.started_at.desc()",
    )
    baseline_scan: Mapped[Optional["Scan"]] = relationship(
        foreign_keys=[baseline_scan_id], post_update=True,
    )


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[int] = mapped_column(primary_key=True)
    target_id: Mapped[int] = mapped_column(
        ForeignKey("targets.id", ondelete="CASCADE")
    )
    cron: Mapped[str] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    target: Mapped[Target] = relationship(
        back_populates="schedules", foreign_keys=[target_id],
    )


class Scan(Base):
    __tablename__ = "scans"

    id: Mapped[int] = mapped_column(primary_key=True)
    target_id: Mapped[int] = mapped_column(
        ForeignKey("targets.id", ondelete="CASCADE")
    )
    trigger: Mapped[str] = mapped_column(String(16), default="manual")
    status: Mapped[str] = mapped_column(String(16), default=SCAN_PENDING)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    files_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bytes_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    peak_rss_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    digest_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    acknowledged: Mapped[bool] = mapped_column(default=False)

    target: Mapped[Target] = relationship(
        back_populates="scans", foreign_keys=[target_id],
    )
    events: Mapped[list["IntegrityEvent"]] = relationship(
        back_populates="scan", cascade="all, delete-orphan",
    )


class IntegrityEvent(Base):
    __tablename__ = "integrity_events"
    __table_args__ = (
        UniqueConstraint("scan_id", "path", name="uq_event_scan_path"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scan_id: Mapped[int] = mapped_column(
        ForeignKey("scans.id", ondelete="CASCADE")
    )
    event_type: Mapped[str] = mapped_column(String(16))
    path: Mapped[str] = mapped_column(Text)
    old_digest: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    new_digest: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    scan: Mapped[Scan] = relationship(back_populates="events")

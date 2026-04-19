from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean, ForeignKey, Index, Integer, String, Text, UniqueConstraint,
)
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

DEDUP_ALGORITHMS = ("xxh3", "xxh128", "sha256")

DEDUP_PENDING = "pending"
DEDUP_RUNNING = "running"
DEDUP_OK = "ok"
DEDUP_DUPLICATES = "duplicates"
DEDUP_FAILED = "failed"
DEDUP_CANCELLED = "cancelled"

DELETION_QUARANTINED = "quarantined"
DELETION_RESTORED = "restored"
DELETION_PURGED = "purged"
DELETION_FAILED = "failed"

STORED_MODE_RENAME = "rename"
STORED_MODE_COPY_UNLINK = "copy_unlink"


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


class DedupTarget(Base):
    __tablename__ = "dedup_targets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    path: Mapped[str] = mapped_column(Text)
    algorithm: Mapped[str] = mapped_column(String(16), default="xxh3")
    threads: Mapped[str] = mapped_column(String(8), default="auto")
    includes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    excludes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    minimum_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    include_hidden: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False,
    )
    follow_symlinks: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False,
    )
    match_hardlinks: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False,
    )
    one_file_system: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False,
    )
    last_scan_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("dedup_scans.id", use_alter=True, ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    schedules: Mapped[list["DedupSchedule"]] = relationship(
        back_populates="target", cascade="all, delete-orphan",
        foreign_keys="DedupSchedule.target_id",
    )
    scans: Mapped[list["DedupScan"]] = relationship(
        back_populates="target", cascade="all, delete-orphan",
        foreign_keys="DedupScan.target_id",
        order_by="DedupScan.started_at.desc()",
    )
    last_scan: Mapped[Optional["DedupScan"]] = relationship(
        foreign_keys=[last_scan_id], post_update=True,
    )


class DedupSchedule(Base):
    __tablename__ = "dedup_schedules"

    id: Mapped[int] = mapped_column(primary_key=True)
    target_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_targets.id", ondelete="CASCADE")
    )
    cron: Mapped[str] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    target: Mapped[DedupTarget] = relationship(
        back_populates="schedules", foreign_keys=[target_id],
    )


class DedupScan(Base):
    __tablename__ = "dedup_scans"

    id: Mapped[int] = mapped_column(primary_key=True)
    target_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_targets.id", ondelete="CASCADE")
    )
    trigger: Mapped[str] = mapped_column(String(16), default="manual")
    status: Mapped[str] = mapped_column(String(16), default=DEDUP_PENDING)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wall_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    files_scanned: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duplicate_groups: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duplicate_files: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wasted_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    peak_rss_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    output_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    acknowledged: Mapped[bool] = mapped_column(default=False)

    target: Mapped[DedupTarget] = relationship(
        back_populates="scans", foreign_keys=[target_id],
    )
    groups: Mapped[list["DedupGroup"]] = relationship(
        back_populates="scan", cascade="all, delete-orphan",
        order_by="DedupGroup.size.desc()",
    )


class DedupGroup(Base):
    __tablename__ = "dedup_groups"
    __table_args__ = (
        Index("ix_dedup_groups_scan_id", "scan_id"),
        Index("ix_dedup_groups_scan_size", "scan_id", "size"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scan_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_scans.id", ondelete="CASCADE")
    )
    size: Mapped[int] = mapped_column(Integer)
    file_count: Mapped[int] = mapped_column(Integer)
    paths_json: Mapped[str] = mapped_column(Text)

    scan: Mapped[DedupScan] = relationship(back_populates="groups")
    deletions: Mapped[list["DedupDeletion"]] = relationship(
        back_populates="group", cascade="all, delete-orphan",
    )


class DedupDeletion(Base):
    __tablename__ = "dedup_deletions"

    id: Mapped[int] = mapped_column(primary_key=True)
    scan_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_scans.id", ondelete="CASCADE")
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_groups.id", ondelete="CASCADE")
    )
    original_path: Mapped[str] = mapped_column(Text)
    trash_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size: Mapped[int] = mapped_column(Integer)
    hash_algo: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    hash_hex: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    stored_mode: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default=DELETION_QUARANTINED)
    triggered_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    deleted_at: Mapped[datetime] = mapped_column(default=utcnow)
    restored_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    purged_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    scan: Mapped[DedupScan] = relationship()
    group: Mapped[DedupGroup] = relationship(back_populates="deletions")

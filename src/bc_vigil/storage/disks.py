from __future__ import annotations

from dataclasses import dataclass

import psutil


@dataclass
class Disk:
    device: str
    mountpoint: str
    fstype: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent: float


def list_disks() -> list[Disk]:
    disks: list[Disk] = []
    for part in psutil.disk_partitions(all=False):
        try:
            usage = psutil.disk_usage(part.mountpoint)
        except (PermissionError, OSError):
            continue
        disks.append(Disk(
            device=part.device,
            mountpoint=part.mountpoint,
            fstype=part.fstype,
            total_bytes=usage.total,
            used_bytes=usage.used,
            free_bytes=usage.free,
            percent=usage.percent,
        ))
    disks.sort(key=lambda d: d.mountpoint)
    return disks

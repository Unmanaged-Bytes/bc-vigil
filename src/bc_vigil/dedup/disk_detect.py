from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


KIND_HDD = "hdd"
KIND_SSD = "ssd"
KIND_REMOVABLE = "removable"
KIND_NETWORK = "network"
KIND_UNKNOWN = "unknown"


NETWORK_FSTYPES: frozenset[str] = frozenset({
    "nfs", "nfs3", "nfs4",
    "cifs", "smbfs", "smb3",
    "9p",
    "ceph", "cephfs",
    "afs",
    "glusterfs",
    "fuse.sshfs",
    "fuse.gvfsd-fuse",
    "fuse.rclone",
})


@dataclass
class DiskInfo:
    kind: str
    fstype: str | None
    source: str | None
    block_device: str | None


def detect_disk_info(path: Path) -> DiskInfo:
    try:
        st = os.stat(path)
    except OSError:
        return DiskInfo(
            kind=KIND_UNKNOWN, fstype=None, source=None, block_device=None,
        )
    mount = _find_mount(st.st_dev)
    if mount is None:
        return DiskInfo(
            kind=KIND_UNKNOWN, fstype=None, source=None, block_device=None,
        )
    fstype = mount["fstype"]
    source = mount["source"]
    if fstype in NETWORK_FSTYPES:
        return DiskInfo(
            kind=KIND_NETWORK, fstype=fstype,
            source=source, block_device=None,
        )
    block = _resolve_block_device(source)
    if block is None:
        return DiskInfo(
            kind=KIND_UNKNOWN, fstype=fstype,
            source=source, block_device=None,
        )
    if _read_sysfs_int(Path("/sys/block") / block / "removable") == 1:
        return DiskInfo(
            kind=KIND_REMOVABLE, fstype=fstype,
            source=source, block_device=block,
        )
    rotational = _read_sysfs_int(
        Path("/sys/block") / block / "queue" / "rotational"
    )
    if rotational == 1:
        return DiskInfo(
            kind=KIND_HDD, fstype=fstype,
            source=source, block_device=block,
        )
    if rotational == 0:
        return DiskInfo(
            kind=KIND_SSD, fstype=fstype,
            source=source, block_device=block,
        )
    return DiskInfo(
        kind=KIND_UNKNOWN, fstype=fstype,
        source=source, block_device=block,
    )


def _find_mount(dev_id: int) -> dict | None:
    major = os.major(dev_id)
    minor = os.minor(dev_id)
    target = f"{major}:{minor}"
    try:
        with open("/proc/self/mountinfo") as fh:
            for raw in fh:
                parts = raw.split()
                if len(parts) < 10:
                    continue
                if parts[2] != target:
                    continue
                try:
                    dash = parts.index("-")
                except ValueError:
                    continue
                if dash + 2 >= len(parts):
                    continue
                return {
                    "mount_point": parts[4],
                    "fstype": parts[dash + 1],
                    "source": parts[dash + 2],
                }
    except OSError:
        return None
    return None


def _resolve_block_device(source: str | None) -> str | None:
    if not source or not source.startswith("/dev/"):
        return None
    name = source[len("/dev/"):]
    if (Path("/sys/block") / name).exists():
        return name
    cls_path = Path("/sys/class/block") / name
    try:
        resolved = cls_path.resolve(strict=True)
    except (OSError, FileNotFoundError):
        return None
    parent = resolved.parent
    if (parent / "queue" / "rotational").exists():
        return parent.name
    return None


def _read_sysfs_int(path: Path) -> int | None:
    try:
        raw = path.read_text().strip()
    except OSError:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def kind_label(kind: str, lang: str = "fr") -> str:
    if lang == "en":
        return {
            KIND_HDD: "HDD (rotational)",
            KIND_SSD: "SSD",
            KIND_REMOVABLE: "removable",
            KIND_NETWORK: "network filesystem",
            KIND_UNKNOWN: "unknown",
        }.get(kind, kind)
    return {
        KIND_HDD: "disque mécanique (HDD)",
        KIND_SSD: "SSD",
        KIND_REMOVABLE: "amovible",
        KIND_NETWORK: "système de fichiers réseau",
        KIND_UNKNOWN: "inconnu",
    }.get(kind, kind)

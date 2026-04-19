from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pytest


# =========================================================================
# disk_detect
# =========================================================================


def test_detect_stat_failure(tmp_path):
    from bc_vigil.dedup import disk_detect
    info = disk_detect.detect_disk_info(tmp_path / "does-not-exist")
    assert info.kind == disk_detect.KIND_UNKNOWN


def test_detect_mount_missing(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(disk_detect, "_find_mount", lambda dev: None)
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_UNKNOWN


def test_detect_network_fs(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/mnt/nas", "fstype": "nfs4",
            "source": "nas:/export",
        },
    )
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_NETWORK
    assert info.fstype == "nfs4"


def test_detect_block_unresolvable(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/", "fstype": "ext4", "source": "/dev/not-a-device",
        },
    )
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_UNKNOWN


def test_detect_hdd(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/", "fstype": "ext4", "source": "/dev/sdx1",
        },
    )
    monkeypatch.setattr(
        disk_detect, "_resolve_block_device", lambda s: "sdx",
    )
    vals = {
        Path("/sys/block/sdx/removable"): 0,
        Path("/sys/block/sdx/queue/rotational"): 1,
    }
    monkeypatch.setattr(disk_detect, "_read_sysfs_int", lambda p: vals.get(p))
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_HDD
    assert info.block_device == "sdx"


def test_detect_ssd(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/", "fstype": "ext4", "source": "/dev/nvme0n1p2",
        },
    )
    monkeypatch.setattr(
        disk_detect, "_resolve_block_device", lambda s: "nvme0n1",
    )
    vals = {
        Path("/sys/block/nvme0n1/removable"): 0,
        Path("/sys/block/nvme0n1/queue/rotational"): 0,
    }
    monkeypatch.setattr(disk_detect, "_read_sysfs_int", lambda p: vals.get(p))
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_SSD


def test_detect_removable(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/media/usb", "fstype": "vfat",
            "source": "/dev/sdy1",
        },
    )
    monkeypatch.setattr(
        disk_detect, "_resolve_block_device", lambda s: "sdy",
    )
    monkeypatch.setattr(disk_detect, "_read_sysfs_int", lambda p: 1)
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_REMOVABLE


def test_detect_rotational_unknown(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    monkeypatch.setattr(
        disk_detect, "_find_mount",
        lambda dev: {
            "mount_point": "/", "fstype": "ext4", "source": "/dev/sdz1",
        },
    )
    monkeypatch.setattr(
        disk_detect, "_resolve_block_device", lambda s: "sdz",
    )
    monkeypatch.setattr(disk_detect, "_read_sysfs_int", lambda p: None)
    info = disk_detect.detect_disk_info(tmp_path)
    assert info.kind == disk_detect.KIND_UNKNOWN


# --- low-level helpers ---------------------------------------------------


def test_find_mount_parses_real_mountinfo(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake = tmp_path / "mountinfo"
    fake.write_text(
        "25 1 0:23 / /proc rw,nosuid - proc proc rw\n"
        "26 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro\n"
        "27 1 8:17 / /data rw - ext4 /dev/sdb1 rw\n"
    )
    real_open = open

    def fake_open(path, *a, **kw):
        if path == "/proc/self/mountinfo":
            return real_open(fake, *a, **kw)
        return real_open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", fake_open)
    import os as _os
    dev = _os.makedev(8, 1)
    m = disk_detect._find_mount(dev)
    assert m is not None
    assert m["fstype"] == "ext4"
    assert m["source"] == "/dev/sda1"


def test_find_mount_no_match(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake = tmp_path / "mountinfo"
    fake.write_text(
        "25 1 99:99 / /other rw - ext4 /dev/sdq1 rw\n"
    )
    real_open = open
    monkeypatch.setattr(
        "builtins.open",
        lambda path, *a, **kw: real_open(fake, *a, **kw)
        if path == "/proc/self/mountinfo" else real_open(path, *a, **kw),
    )
    import os as _os
    m = disk_detect._find_mount(_os.makedev(8, 1))
    assert m is None


def test_find_mount_short_lines(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake = tmp_path / "mountinfo"
    fake.write_text("too short\n8 1 8:1 something\n")
    real_open = open
    monkeypatch.setattr(
        "builtins.open",
        lambda path, *a, **kw: real_open(fake, *a, **kw)
        if path == "/proc/self/mountinfo" else real_open(path, *a, **kw),
    )
    import os as _os
    assert disk_detect._find_mount(_os.makedev(8, 1)) is None


def test_find_mount_no_dash_separator(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake = tmp_path / "mountinfo"
    fake.write_text(
        "25 1 8:1 / / rw,relatime shared:1 NOTDASH ext4 /dev/sda1 rw\n"
    )
    real_open = open
    monkeypatch.setattr(
        "builtins.open",
        lambda path, *a, **kw: real_open(fake, *a, **kw)
        if path == "/proc/self/mountinfo" else real_open(path, *a, **kw),
    )
    import os as _os
    assert disk_detect._find_mount(_os.makedev(8, 1)) is None


def test_find_mount_dash_near_end(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake = tmp_path / "mountinfo"
    # First line: 10 fields, dash at index 9 (the last), dash+2 = 11 >= 10
    # -> triggers the "dash near end" continue on line 102.
    # Second line is a valid match so loop can return something after.
    fake.write_text(
        "25 1 8:1 / /mnt rw shared:1 opt1 opt2 -\n"
        "26 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"
    )
    real_open = open
    monkeypatch.setattr(
        "builtins.open",
        lambda path, *a, **kw: real_open(fake, *a, **kw)
        if path == "/proc/self/mountinfo" else real_open(path, *a, **kw),
    )
    import os as _os
    m = disk_detect._find_mount(_os.makedev(8, 1))
    assert m is not None
    assert m["fstype"] == "ext4"


def test_find_mount_oserror(monkeypatch):
    from bc_vigil.dedup import disk_detect

    def bad(path, *a, **kw):
        if path == "/proc/self/mountinfo":
            raise OSError("denied")
        return open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", bad)
    import os as _os
    assert disk_detect._find_mount(_os.makedev(8, 1)) is None


def test_resolve_block_device_direct(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    fake_root = tmp_path / "sys"
    (fake_root / "block" / "myblock").mkdir(parents=True)
    import pathlib
    original_exists = pathlib.Path.exists

    def fake_exists(self):
        if str(self).startswith("/sys/block/"):
            name = self.name
            return (fake_root / "block" / name).exists()
        return original_exists(self)

    monkeypatch.setattr(pathlib.Path, "exists", fake_exists)
    out = disk_detect._resolve_block_device("/dev/myblock")
    assert out == "myblock"


def test_resolve_block_device_via_class(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    root = tmp_path / "sys"
    (root / "devices" / "x" / "sda").mkdir(parents=True)
    (root / "devices" / "x" / "sda" / "queue").mkdir()
    (root / "devices" / "x" / "sda" / "queue" / "rotational").write_text("1")
    (root / "devices" / "x" / "sda" / "sda1").mkdir()
    (root / "class" / "block").mkdir(parents=True)
    (root / "class" / "block" / "sda1").symlink_to(
        root / "devices" / "x" / "sda" / "sda1"
    )
    import pathlib
    original_exists = pathlib.Path.exists
    original_resolve = pathlib.Path.resolve

    def fake_exists(self):
        mapped = _map(self)
        if mapped is not None:
            return mapped.exists()
        return original_exists(self)

    def fake_resolve(self, strict=False):
        mapped = _map(self)
        if mapped is not None:
            return mapped.resolve(strict=strict)
        return original_resolve(self, strict=strict)

    def _map(p):
        s = str(p)
        if s.startswith("/sys/"):
            return tmp_path / "sys" / Path(s[len("/sys/"):])
        return None

    monkeypatch.setattr(pathlib.Path, "exists", fake_exists)
    monkeypatch.setattr(pathlib.Path, "resolve", fake_resolve)
    out = disk_detect._resolve_block_device("/dev/sda1")
    assert out == "sda"


def test_resolve_block_device_not_dev():
    from bc_vigil.dedup import disk_detect
    assert disk_detect._resolve_block_device("nas:/export") is None
    assert disk_detect._resolve_block_device(None) is None


def test_resolve_block_device_class_resolve_oserror(monkeypatch):
    from bc_vigil.dedup import disk_detect
    import pathlib
    original_exists = pathlib.Path.exists
    original_resolve = pathlib.Path.resolve

    def fake_exists(self):
        if str(self) == "/sys/block/xxxxyyy":
            return False
        return original_exists(self)

    def fake_resolve(self, strict=False):
        if "xxxxyyy" in str(self):
            raise OSError("denied")
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(pathlib.Path, "exists", fake_exists)
    monkeypatch.setattr(pathlib.Path, "resolve", fake_resolve)
    assert disk_detect._resolve_block_device("/dev/xxxxyyy") is None


def test_resolve_block_device_parent_no_queue(tmp_path, monkeypatch):
    from bc_vigil.dedup import disk_detect
    import pathlib
    root = tmp_path / "sys"
    (root / "devices" / "y" / "noqueuedisk").mkdir(parents=True)
    (root / "devices" / "y" / "noqueuedisk" / "part1").mkdir()
    (root / "class" / "block").mkdir(parents=True)
    (root / "class" / "block" / "part1").symlink_to(
        root / "devices" / "y" / "noqueuedisk" / "part1"
    )
    original_exists = pathlib.Path.exists
    original_resolve = pathlib.Path.resolve

    def _map(p):
        s = str(p)
        if s.startswith("/sys/"):
            return tmp_path / "sys" / Path(s[len("/sys/"):])
        return None

    monkeypatch.setattr(
        pathlib.Path, "exists",
        lambda self: _map(self).exists() if _map(self) else original_exists(self),
    )
    monkeypatch.setattr(
        pathlib.Path, "resolve",
        lambda self, strict=False: _map(self).resolve(strict=strict)
        if _map(self) else original_resolve(self, strict=strict),
    )
    assert disk_detect._resolve_block_device("/dev/part1") is None


def test_read_sysfs_int_missing(tmp_path):
    from bc_vigil.dedup import disk_detect
    assert disk_detect._read_sysfs_int(tmp_path / "nope") is None


def test_read_sysfs_int_non_integer(tmp_path):
    from bc_vigil.dedup import disk_detect
    p = tmp_path / "weird"
    p.write_text("not-a-number")
    assert disk_detect._read_sysfs_int(p) is None


def test_read_sysfs_int_ok(tmp_path):
    from bc_vigil.dedup import disk_detect
    p = tmp_path / "v"
    p.write_text("1")
    assert disk_detect._read_sysfs_int(p) == 1


def test_kind_label():
    from bc_vigil.dedup import disk_detect
    assert disk_detect.kind_label("hdd") == "disque mécanique (HDD)"
    assert disk_detect.kind_label("hdd", "en") == "HDD (rotational)"
    assert disk_detect.kind_label("ssd") == "SSD"
    assert disk_detect.kind_label("ssd", "en") == "SSD"
    assert disk_detect.kind_label("nope") == "nope"
    assert disk_detect.kind_label("nope", "en") == "nope"


# =========================================================================
# routes/targets: HDD forcing + network opt-in
# =========================================================================


def _mock_disk(monkeypatch, kind: str):
    from bc_vigil.dedup import disk_detect
    info = disk_detect.DiskInfo(
        kind=kind,
        fstype="ext4" if kind != "network" else "nfs4",
        source="/dev/sdx1" if kind != "network" else "nas:/export",
        block_device="sdx" if kind not in ("network",) else None,
    )
    monkeypatch.setattr(disk_detect, "detect_disk_info", lambda p: info)


def test_create_target_on_hdd_forces_threads_1(tmp_path, monkeypatch):
    folder = tmp_path / "target-hdd"
    folder.mkdir()
    _mock_disk(monkeypatch, "hdd")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "hdd-target", "path": str(folder),
                "algorithm": "xxh3", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = session.scalar(
            __import__("sqlalchemy").select(models.DedupTarget).where(
                models.DedupTarget.name == "hdd-target"
            )
        )
        assert t.threads == "1"


def test_create_target_on_hdd_override_keeps_threads(tmp_path, monkeypatch):
    folder = tmp_path / "target-hdd-override"
    folder.mkdir()
    _mock_disk(monkeypatch, "hdd")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "hdd-override", "path": str(folder),
                "algorithm": "xxh3", "threads": "4",
                "advanced_threads_override": "true",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = session.scalar(
            __import__("sqlalchemy").select(models.DedupTarget).where(
                models.DedupTarget.name == "hdd-override"
            )
        )
        assert t.threads == "4"


def test_create_target_on_network_requires_opt_in(tmp_path, monkeypatch):
    folder = tmp_path / "target-nfs"
    folder.mkdir()
    _mock_disk(monkeypatch, "network")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "nfs-target", "path": str(folder),
                "algorithm": "xxh3", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "réseau" in r.text


def test_create_target_on_network_with_opt_in(tmp_path, monkeypatch):
    folder = tmp_path / "target-nfs-ok"
    folder.mkdir()
    _mock_disk(monkeypatch, "network")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "nfs-ok", "path": str(folder),
                "algorithm": "xxh3", "threads": "auto",
                "accept_network_fs": "true",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303


def test_create_target_ssd_no_forcing(tmp_path, monkeypatch):
    folder = tmp_path / "target-ssd"
    folder.mkdir()
    _mock_disk(monkeypatch, "ssd")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "ssd-target", "path": str(folder),
                "algorithm": "xxh3", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = session.scalar(
            __import__("sqlalchemy").select(models.DedupTarget).where(
                models.DedupTarget.name == "ssd-target"
            )
        )
        assert t.threads == "auto"


def test_show_target_displays_disk(tmp_path, monkeypatch):
    _mock_disk(monkeypatch, "hdd")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / "target-show"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="show", path=str(folder), algorithm="xxh3", threads="1",
        )
        session.add(t); session.commit()
        tid = t.id

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/targets/{tid}")
        assert r.status_code == 200
        assert "disque mécanique" in r.text or "HDD" in r.text


# =========================================================================
# trigger_scan lock: ScanAlreadyRunningError
# =========================================================================


def test_trigger_scan_refuses_concurrent(tmp_path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    from bc_vigil.dedup import scans

    folder = tmp_path / "lock-t"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="lock-t", path=str(folder), algorithm="xxh3", threads="auto",
        )
        session.add(t); session.commit()
        tid = t.id

    first = scans.trigger_scan(tid)
    with pytest.raises(scans.ScanAlreadyRunningError) as ei:
        scans.trigger_scan(tid)
    assert ei.value.active_scan_id == first


def test_run_scan_route_redirects_to_active_scan(tmp_path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    from bc_vigil.dedup import scans

    folder = tmp_path / "lock-route"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="lock-route", path=str(folder), algorithm="xxh3", threads="auto",
        )
        session.add(t); session.commit()
        tid = t.id

    first = scans.trigger_scan(tid)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/run?target_id={tid}", follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == f"/dedup/scans/{first}"


def test_scheduled_run_skips_when_active(tmp_path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    from bc_vigil.dedup import scans, scheduler

    folder = tmp_path / "sched-active"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="sched-active", path=str(folder),
            algorithm="xxh3", threads="auto",
        )
        session.add(t); session.commit()
        s = models.DedupSchedule(
            target_id=t.id, cron="0 3 * * *", enabled=True,
        )
        session.add(s); session.commit()
        sid = s.id
        tid = t.id

    scans.trigger_scan(tid)
    scheduler._run_scheduled_scan(sid)

    with SessionLocal() as session:
        count = session.query(models.DedupScan).filter_by(
            target_id=tid
        ).count()
        assert count == 1

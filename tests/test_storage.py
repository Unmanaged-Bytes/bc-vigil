from __future__ import annotations

from dataclasses import dataclass


@dataclass
class _FakePart:
    device: str
    mountpoint: str
    fstype: str
    opts: str = "rw"


@dataclass
class _FakeUsage:
    total: int
    used: int
    free: int
    percent: float


def test_list_disks_maps_psutil_output(monkeypatch):
    from bc_vigil.storage import disks

    fake_parts = [
        _FakePart(device="/dev/nvme0n1p2", mountpoint="/", fstype="ext4"),
        _FakePart(device="/dev/nvme0n1p1", mountpoint="/boot", fstype="vfat"),
    ]
    usage_map = {
        "/": _FakeUsage(total=500_000_000_000, used=250_000_000_000, free=250_000_000_000, percent=50.0),
        "/boot": _FakeUsage(total=1_000_000_000, used=200_000_000, free=800_000_000, percent=20.0),
    }

    import psutil
    monkeypatch.setattr(psutil, "disk_partitions", lambda all=False: fake_parts)
    monkeypatch.setattr(psutil, "disk_usage", lambda path: usage_map[path])

    result = disks.list_disks()
    assert len(result) == 2
    # sorted by mountpoint
    assert result[0].mountpoint == "/"
    assert result[1].mountpoint == "/boot"
    assert result[0].device == "/dev/nvme0n1p2"
    assert result[0].fstype == "ext4"
    assert result[0].total_bytes == 500_000_000_000
    assert result[0].percent == 50.0


def test_list_disks_skips_permission_errors(monkeypatch):
    from bc_vigil.storage import disks

    fake_parts = [
        _FakePart(device="/dev/ok", mountpoint="/ok", fstype="ext4"),
        _FakePart(device="/dev/denied", mountpoint="/denied", fstype="ext4"),
    ]

    def usage(path):
        if "denied" in path:
            raise PermissionError("nope")
        return _FakeUsage(total=100, used=10, free=90, percent=10.0)

    import psutil
    monkeypatch.setattr(psutil, "disk_partitions", lambda all=False: fake_parts)
    monkeypatch.setattr(psutil, "disk_usage", usage)

    result = disks.list_disks()
    assert len(result) == 1
    assert result[0].mountpoint == "/ok"


def test_list_disks_skips_oserror(monkeypatch):
    from bc_vigil.storage import disks

    fake_parts = [_FakePart(device="/dev/bad", mountpoint="/bad", fstype="ext4")]

    def usage(path):
        raise OSError("io error")

    import psutil
    monkeypatch.setattr(psutil, "disk_partitions", lambda all=False: fake_parts)
    monkeypatch.setattr(psutil, "disk_usage", usage)

    assert disks.list_disks() == []


def test_storage_route_renders_table(monkeypatch):
    from bc_vigil.storage import disks

    fake = [
        disks.Disk(
            device="/dev/x", mountpoint="/", fstype="ext4",
            total_bytes=10_737_418_240, used_bytes=5_368_709_120,
            free_bytes=5_368_709_120, percent=50.0,
        ),
    ]
    monkeypatch.setattr(disks, "list_disks", lambda: fake)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/storage")
        assert r.status_code == 200
        assert "/dev/x" in r.text
        assert "ext4" in r.text
        assert "50.0%" in r.text


def test_storage_route_renders_empty_state(monkeypatch):
    from bc_vigil.storage import disks
    monkeypatch.setattr(disks, "list_disks", lambda: [])

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/storage")
        assert r.status_code == 200


def test_storage_route_marks_high_usage(monkeypatch):
    from bc_vigil.storage import disks

    fake = [
        disks.Disk(
            device="/dev/full", mountpoint="/full", fstype="ext4",
            total_bytes=100, used_bytes=95, free_bytes=5, percent=95.0,
        ),
    ]
    monkeypatch.setattr(disks, "list_disks", lambda: fake)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/storage")
        assert "critical" in r.text


def test_nav_contains_storage_link(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/")
        assert 'href="/storage"' in r.text
        assert "Stockage" in r.text


def test_nav_storage_english(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        client.cookies.set("bcv_lang", "en")
        r = client.get("/")
        assert "Storage" in r.text

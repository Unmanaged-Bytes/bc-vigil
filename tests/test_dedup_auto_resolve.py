from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _seed_target_and_scan(
    tmp_path: Path,
    name: str,
    groups: list[list[tuple[str, bytes, float | None]]],
) -> tuple[int, list[int]]:
    """
    Each group is a list of (relative_path, content, mtime_or_none).
    Returns (scan_id, [group_ids]).
    """
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    tree = tmp_path / f"tree-{name}"
    tree.mkdir(exist_ok=True)

    with SessionLocal() as session:
        target = models.DedupTarget(
            name=name, path=str(tree), algorithm="xxh3", threads="auto",
        )
        session.add(target)
        session.flush()
        scan = models.DedupScan(
            target_id=target.id, status=models.DEDUP_DUPLICATES,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        )
        session.add(scan)
        session.flush()

        gids: list[int] = []
        for i, files in enumerate(groups):
            paths_on_disk = []
            for rel, content, mtime in files:
                p = tree / f"{i}-{rel}"
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(content)
                if mtime is not None:
                    os.utime(p, (mtime, mtime))
                paths_on_disk.append(str(p))
            g = models.DedupGroup(
                scan_id=scan.id, size=len(files[0][1]),
                file_count=len(files),
                paths_json=json.dumps(paths_on_disk),
            )
            session.add(g)
            session.flush()
            gids.append(g.id)
        session.commit()
        return scan.id, gids


# =========================================================================
# build_auto_selection
# =========================================================================


def test_auto_unknown_rule():
    from bc_vigil.dedup import quarantine
    with pytest.raises(quarantine.QuarantineError, match="unknown auto rule"):
        quarantine.build_auto_selection(1, "nope")


def test_auto_priority_folder_without_path():
    from bc_vigil.dedup import quarantine
    with pytest.raises(quarantine.QuarantineError, match="priority_path"):
        quarantine.build_auto_selection(1, "priority_folder")


def test_auto_scan_missing():
    from bc_vigil.dedup import quarantine
    with pytest.raises(quarantine.QuarantineError, match="not found"):
        quarantine.build_auto_selection(999999, "shortest_path")


def test_auto_scan_wrong_status(tmp_path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="auto-ok", path=str(tmp_path), algorithm="xxh3", threads="auto",
        )
        session.add(t); session.flush()
        s = models.DedupScan(target_id=t.id, status=models.DEDUP_OK)
        session.add(s); session.commit()
        sid = s.id
    from bc_vigil.dedup import quarantine
    with pytest.raises(quarantine.QuarantineError, match="not duplicates"):
        quarantine.build_auto_selection(sid, "shortest_path")


def test_auto_shortest_path(tmp_path):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "shortest",
        [[
            ("long-path/very-deep/file.bin", b"hello", None),
            ("a.bin", b"hello", None),
            ("med/b.bin", b"hello", None),
        ]],
    )
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(scan_id, "shortest_path")
    victims = sel[gids[0]]
    # survivor should be "a.bin" (shortest name overall); the other two become victims
    assert len(victims) == 2
    assert not any(v.endswith("0-a.bin") for v in victims)


def test_auto_oldest_mtime(tmp_path):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "oldest",
        [[
            ("recent.bin", b"hello", time.time()),
            ("middle.bin", b"hello", time.time() - 3600),
            ("old.bin", b"hello", time.time() - 7200),
        ]],
    )
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(scan_id, "oldest_mtime")
    victims = sel[gids[0]]
    assert len(victims) == 2
    assert not any(v.endswith("0-old.bin") for v in victims)


def test_auto_newest_mtime(tmp_path):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "newest",
        [[
            ("fresh.bin", b"hello", time.time()),
            ("stale.bin", b"hello", time.time() - 7200),
        ]],
    )
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(scan_id, "newest_mtime")
    victims = sel[gids[0]]
    assert len(victims) == 1
    assert victims[0].endswith("0-stale.bin")


def test_auto_mtime_all_unreadable(tmp_path, monkeypatch):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "mtime-fail",
        [[
            ("a.bin", b"hello", None),
            ("b.bin", b"hello", None),
        ]],
    )
    from bc_vigil.dedup import quarantine
    import pathlib
    original_stat = pathlib.Path.stat

    def bad_stat(self, *a, **kw):
        if self.name.startswith("0-"):
            raise OSError("EACCES")
        return original_stat(self, *a, **kw)

    monkeypatch.setattr(pathlib.Path, "stat", bad_stat)
    sel = quarantine.build_auto_selection(scan_id, "oldest_mtime")
    # group produced no survivor -> skipped entirely
    assert sel == {}


def test_auto_priority_folder_hits(tmp_path):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "priority",
        [[("a.bin", b"hello", None), ("b.bin", b"hello", None)]],
    )
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(
        scan_id, "priority_folder",
        priority_path=str(tmp_path / "tree-priority"),
    )
    assert gids[0] in sel
    victims = sel[gids[0]]
    assert len(victims) == 1


def test_auto_priority_folder_no_match(tmp_path):
    scan_id, gids = _seed_target_and_scan(
        tmp_path, "priority-miss",
        [[("a.bin", b"hello", None), ("b.bin", b"hello", None)]],
    )
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(
        scan_id, "priority_folder",
        priority_path="/nowhere",
    )
    assert sel == {}


def test_auto_skips_singleton_groups(tmp_path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = models.DedupTarget(
            name="single", path=str(tmp_path),
            algorithm="xxh3", threads="auto",
        )
        session.add(t); session.flush()
        s = models.DedupScan(target_id=t.id, status=models.DEDUP_DUPLICATES)
        session.add(s); session.flush()
        g = models.DedupGroup(
            scan_id=s.id, size=10, file_count=1,
            paths_json=json.dumps(["/only-one"]),
        )
        session.add(g); session.commit()
        sid = s.id
    from bc_vigil.dedup import quarantine
    sel = quarantine.build_auto_selection(sid, "shortest_path")
    assert sel == {}


# =========================================================================
# routes: auto-resolve + pagination
# =========================================================================


def test_route_auto_resolve_missing_scan():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/scans/99999/auto-resolve",
            data={"rule": "shortest_path"},
            follow_redirects=False,
        )
        assert r.status_code == 404


def test_route_auto_resolve_unknown_rule(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "rt-auto-bad",
        [[("a.bin", b"hello", None), ("b.bin", b"hello", None)]],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/{scan_id}/auto-resolve",
            data={"rule": "nonsense"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_auto_resolve_empty_selection(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "rt-auto-empty",
        [[("a.bin", b"hello", None), ("b.bin", b"hello", None)]],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/{scan_id}/auto-resolve",
            data={"rule": "priority_folder", "priority_path": "/nowhere"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "éligible" in r.text


def test_route_auto_resolve_plan_error(tmp_path, monkeypatch):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "rt-auto-plan-err",
        [[("a.bin", b"hello", None), ("b.bin", b"hello", None)]],
    )
    from bc_vigil.dedup import quarantine

    def boom(*a, **kw):
        raise quarantine.QuarantineError("planning failed")

    monkeypatch.setattr(quarantine, "plan_deletion", boom)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/{scan_id}/auto-resolve",
            data={"rule": "shortest_path"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_auto_resolve_happy(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "rt-auto-happy",
        [[("a.bin", b"hello", None), ("bb.bin", b"hello", None)]],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/{scan_id}/auto-resolve",
            data={"rule": "shortest_path"},
            follow_redirects=False,
        )
        assert r.status_code == 200


def test_show_scan_pagination_defaults(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "pag-default",
        [
            [("x.bin", b"aa" * (i + 1), None), ("y.bin", b"aa" * (i + 1), None)]
            for i in range(3)
        ],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}")
        assert r.status_code == 200


def test_show_scan_pagination_clamp(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "pag-clamp",
        [[("a.bin", b"1", None), ("b.bin", b"1", None)]],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}?page=0&per_page=0")
        assert r.status_code == 200
        r = client.get(f"/dedup/scans/{scan_id}?page=1&per_page=500")
        assert r.status_code == 200


def test_show_scan_pagination_sort_asc(tmp_path):
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "pag-sort",
        [[("a.bin", b"1", None), ("b.bin", b"1", None)]],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}?sort=size_asc")
        assert r.status_code == 200
        r = client.get(f"/dedup/scans/{scan_id}?sort=bogus")
        assert r.status_code == 200


def test_show_scan_large_warning(tmp_path, monkeypatch):
    import bc_vigil.dedup.routes.scans as scans_mod
    monkeypatch.setattr(scans_mod, "_LARGE_SCAN_THRESHOLD", 1)
    scan_id, _ = _seed_target_and_scan(
        tmp_path, "pag-large",
        [[("a.bin", b"1", None), ("b.bin", b"1", None)] for _ in range(3)],
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}")
        assert r.status_code == 200
        # banner text in FR
        assert "résolution automatique" in r.text.lower()


# =========================================================================
# db: WAL pragma
# =========================================================================


def test_wal_pragma_enabled():
    from bc_vigil.db import engine
    with engine.connect() as conn:
        mode = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
    assert mode.lower() == "wal"


def test_wal_checkpoint_helper_swallows(monkeypatch):
    from bc_vigil.core import admin_ops
    from bc_vigil import db as db_module

    class FakeEngine:
        def connect(self):
            raise RuntimeError("no db")

    monkeypatch.setattr(db_module, "engine", FakeEngine())
    admin_ops._checkpoint_wal()


# =========================================================================
# bulk insert
# =========================================================================


def test_bulk_insert_groups(tmp_path):
    from bc_vigil import models
    from bc_vigil.dedup import bcduplicate, scans
    from bc_vigil.db import SessionLocal

    with SessionLocal() as session:
        t = models.DedupTarget(
            name="bulk", path=str(tmp_path), algorithm="xxh3", threads="auto",
        )
        session.add(t); session.flush()
        s = models.DedupScan(target_id=t.id, status=models.DEDUP_DUPLICATES)
        session.add(s); session.commit()
        sid = s.id

    # Build 5000 fake groups to trigger 3 chunks (chunk size = 2000)
    fake_groups = [
        bcduplicate.DuplicateGroup(size=10 + i, files=[f"/a/{i}", f"/b/{i}"])
        for i in range(5000)
    ]
    scans._bulk_insert_groups(sid, fake_groups)
    with SessionLocal() as session:
        count = session.query(models.DedupGroup).filter_by(scan_id=sid).count()
        assert count == 5000


def test_bulk_insert_empty_noop():
    from bc_vigil.dedup import scans
    # just make sure no exception
    scans._bulk_insert_groups(1, [])

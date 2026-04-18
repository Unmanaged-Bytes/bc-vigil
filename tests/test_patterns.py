from __future__ import annotations

from pathlib import Path

import pytest


def test_parse_patterns():
    from bc_vigil.integrity import bchash
    assert bchash.parse_patterns(None) == []
    assert bchash.parse_patterns("") == []
    assert bchash.parse_patterns("*.log") == ["*.log"]
    assert bchash.parse_patterns("  *.log  \n\n.git\n  node_modules\n") == [
        "*.log", ".git", "node_modules",
    ]


def test_run_hash_forwards_include_exclude_flags(tmp_path, monkeypatch):
    captured_args = tmp_path / "captured-args.txt"
    recorder = tmp_path / "bc-hash-recorder"
    recorder.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s\\n' \"$@\" > {captured_args}\n"
        "OUT=\"\"\n"
        "for a in \"$@\"; do case \"$a\" in --output=*) OUT=\"${a#--output=}\";; esac; done\n"
        "mkdir -p \"$(dirname \"$OUT\")\"\n"
        "printf '%s\\n' '{\"type\":\"header\",\"tool\":\"bc-hash\",\"version\":\"1.0.0\",\"schema_version\":1,\"algorithm\":\"sha256\",\"started_at\":\"2026-01-01T00:00:00Z\"}' > \"$OUT\"\n"
        "printf '%s\\n' '{\"type\":\"summary\",\"files_total\":0,\"files_ok\":0,\"files_error\":0,\"bytes_total\":0,\"wall_ms\":0,\"workers\":1,\"mode\":\"sequential\"}' >> \"$OUT\"\n"
    )
    import stat
    recorder.chmod(recorder.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(recorder)

    from bc_vigil.integrity import bchash
    target = tmp_path / "tree"
    target.mkdir()
    digest = tmp_path / "d.ndjson"

    bchash.run_hash(
        target, digest, "sha256", "auto",
        includes=["*.c", "*.h"],
        excludes=[".git", "node_modules", "*.log"],
    )

    args = captured_args.read_text().splitlines()
    assert "--include=*.c" in args
    assert "--include=*.h" in args
    assert "--exclude=.git" in args
    assert "--exclude=node_modules" in args
    assert "--exclude=*.log" in args
    assert args.index("--include=*.c") < args.index(str(target))


def test_create_target_with_patterns_via_http(tmp_path):
    source = tmp_path / "src"
    source.mkdir()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "patterns",
                "path": str(source),
                "algorithm": "sha256",
                "threads": "auto",
                "excludes": ".git\nnode_modules\n*.log\n",
                "includes": "",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        target = session.query(models.Target).filter_by(name="patterns").one()
        assert target.excludes == ".git\nnode_modules\n*.log"
        assert target.includes is None


@pytest.mark.requires_bchash
def test_scan_respects_exclude_on_real_bchash(tmp_path):
    source = tmp_path / "repo"
    (source / ".git").mkdir(parents=True)
    (source / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (source / "src").mkdir()
    (source / "src" / "main.c").write_text("int main(void){return 0;}\n")
    (source / "trace.log").write_text("noise\n")

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = models.Target(
            name="excluded", path=str(source), algorithm="sha256", threads="auto",
            excludes=".git\n*.log",
        )
        session.add(t)
        session.commit()
        target_id = t.id

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_OK, scan.error
        assert scan.files_total == 1, (
            f"expected only src/main.c (1 file), got {scan.files_total}"
        )

        content = Path(scan.digest_path).read_text()
        assert "main.c" in content
        assert ".git" not in content
        assert "trace.log" not in content

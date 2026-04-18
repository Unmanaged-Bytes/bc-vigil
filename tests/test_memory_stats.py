from __future__ import annotations

import stat
from pathlib import Path


def _install_memory_hog(tmp_path: Path) -> Path:
    script = tmp_path / "bc-hash-memhog"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys, time\n"
        "out = None\n"
        "for a in sys.argv[1:]:\n"
        "    if a.startswith('--output='):\n"
        "        out = a.split('=',1)[1]\n"
        "blob = b'x' * (40 * 1024 * 1024)\n"
        "time.sleep(0.3)\n"
        "if out:\n"
        "    os.makedirs(os.path.dirname(out), exist_ok=True)\n"
        "    with open(out, 'w') as fh:\n"
        "        fh.write('{\"type\":\"header\",\"tool\":\"bc-hash\",\"version\":\"1.0.0\",\"schema_version\":1,\"algorithm\":\"sha256\",\"started_at\":\"2026-01-01T00:00:00Z\"}\\n')\n"
        "        fh.write('{\"type\":\"summary\",\"files_total\":0,\"files_ok\":0,\"files_error\":0,\"bytes_total\":0,\"wall_ms\":300,\"workers\":1,\"mode\":\"sequential\"}\\n')\n"
        "sys.exit(0)\n"
        "_ = len(blob)\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return script


def test_peak_rss_is_measured(tmp_path):
    fake = _install_memory_hog(tmp_path)
    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(fake)

    from bc_vigil.integrity import bchash
    digest = tmp_path / "digest.json"
    result = bchash.run_hash(tmp_path, digest, "sha256", "auto")

    assert result.peak_rss_bytes is not None
    assert result.peak_rss_bytes > 20 * 1024 * 1024, (
        f"expected peak > 20 MiB, got {result.peak_rss_bytes}"
    )


def test_scan_persists_peak_rss(tmp_path):
    fake = _install_memory_hog(tmp_path)
    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(fake)

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal
    folder = tmp_path / "tree"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.Target(name="mem", path=str(folder), algorithm="sha256", threads="auto")
        session.add(t)
        session.commit()
        target_id = t.id

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_OK
        assert scan.peak_rss_bytes is not None
        assert scan.peak_rss_bytes > 20 * 1024 * 1024


def test_humanbytes_filter():
    from bc_vigil.app import _format_bytes
    assert _format_bytes(None) == "—"
    assert _format_bytes(0) == "0 B"
    assert _format_bytes(512) == "512 B"
    assert _format_bytes(1024) == "1.0 KiB"
    assert _format_bytes(1536) == "1.5 KiB"
    assert _format_bytes(1024 * 1024) == "1.0 MiB"
    assert _format_bytes(40 * 1024 * 1024) == "40.0 MiB"

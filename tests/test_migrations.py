from __future__ import annotations

from sqlalchemy import create_engine, inspect, text


def test_add_column_on_existing_db(tmp_path):
    from bc_vigil import db as db_module
    from bc_vigil.config import settings

    db_file = settings.data_dir / "bc-vigil.sqlite"
    db_module.engine.dispose()
    if db_file.exists():
        db_file.unlink()

    raw = create_engine(f"sqlite:///{db_file}")
    with raw.begin() as conn:
        conn.execute(text("""
            CREATE TABLE targets (
                id INTEGER PRIMARY KEY,
                name VARCHAR(128) UNIQUE,
                path TEXT NOT NULL,
                algorithm VARCHAR(16) NOT NULL DEFAULT 'sha256',
                threads VARCHAR(8) NOT NULL DEFAULT 'auto',
                baseline_scan_id INTEGER,
                created_at DATETIME NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE scans (
                id INTEGER PRIMARY KEY,
                target_id INTEGER NOT NULL,
                trigger VARCHAR(16) NOT NULL DEFAULT 'manual',
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                started_at DATETIME NOT NULL,
                finished_at DATETIME,
                duration_ms INTEGER,
                files_total INTEGER,
                bytes_total INTEGER,
                digest_path TEXT,
                error TEXT,
                acknowledged BOOLEAN NOT NULL DEFAULT 0
            )
        """))
        conn.execute(text("""
            CREATE TABLE schedules (
                id INTEGER PRIMARY KEY,
                target_id INTEGER NOT NULL,
                cron VARCHAR(128) NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE integrity_events (
                id INTEGER PRIMARY KEY,
                scan_id INTEGER NOT NULL,
                event_type VARCHAR(16) NOT NULL,
                path TEXT NOT NULL,
                old_digest VARCHAR(64),
                new_digest VARCHAR(64)
            )
        """))
    raw.dispose()

    pre = inspect(create_engine(f"sqlite:///{db_file}")).get_columns("scans")
    assert "peak_rss_bytes" not in {c["name"] for c in pre}

    db_module.reset_engine()
    db_module.init_db()

    post = inspect(db_module.engine).get_columns("scans")
    assert "peak_rss_bytes" in {c["name"] for c in post}

    from bc_vigil import models
    with db_module.SessionLocal() as session:
        session.query(models.Scan).all()

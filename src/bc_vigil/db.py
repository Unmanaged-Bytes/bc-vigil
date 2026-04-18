import logging
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from bc_vigil.config import settings

log = logging.getLogger(__name__)


def _build_engine():
    return create_engine(
        settings.db_url,
        echo=False,
        connect_args={"check_same_thread": False},
    )


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def reset_engine() -> None:
    global engine, SessionLocal
    engine.dispose()
    engine = _build_engine()
    SessionLocal.configure(bind=engine)


def init_db() -> None:
    from bc_vigil import models

    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.digests_dir.mkdir(parents=True, exist_ok=True)
    models.Base.metadata.create_all(engine)
    _add_missing_columns(models.Base)


def _add_missing_columns(base) -> None:
    insp = inspect(engine)
    for table in base.metadata.sorted_tables:
        if not insp.has_table(table.name):
            continue
        existing = {c["name"] for c in insp.get_columns(table.name)}
        for column in table.columns:
            if column.name in existing:
                continue
            col_type = column.type.compile(engine.dialect)
            ddl = (
                f'ALTER TABLE "{table.name}" '
                f'ADD COLUMN "{column.name}" {col_type}'
            )
            with engine.begin() as conn:
                conn.execute(text(ddl))
            log.info("schema migration: added %s.%s", table.name, column.name)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

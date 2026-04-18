from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_db(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))

    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "data_dir", tmp_path / "var")
    monkeypatch.setattr(settings, "bc_hash_binary", "bc-hash")

    from bc_vigil import db as db_module
    db_module.reset_engine()
    db_module.init_db()
    yield

# BitCrafts Vigil — project context

Web frontend (FastAPI + SQLite + APScheduler + Jinja2/HTMX) for the
`bc-hash` binary. Lets operators register file-tree targets, schedule
periodic hashes, and detect integrity drift (`added` / `removed` /
`modified`) against a baseline.

## Runtime dependencies

- Python 3.11+
- `bc-hash` (>= 1.0.0, with `diff` subcommand) available in `$PATH` — the
  web UI is a thin orchestrator on top of this binary
- Python packages: `fastapi`, `uvicorn`, `sqlalchemy`, `apscheduler`,
  `jinja2`, `croniter`, `pydantic-settings`, `psutil`
  (single source of truth: `pyproject.toml`)

## Source layout

| Path | Role |
|---|---|
| `src/bc_vigil/{app,config,db,models,i18n}.py` | shared infra |
| `src/bc_vigil/core/` | dashboard, admin (backup/restore/reset), help, lang switch |
| `src/bc_vigil/integrity/` | `bc-hash` orchestration: targets, scans, schedules |
| `src/bc_vigil/storage/` | disk usage module (psutil) |
| `src/bc_vigil/templates/` | Jinja2 templates, FR/EN via `i18n.py` + cookie |
| `tests/` | pytest suite, **100 % coverage gate** |
| `debian/` | `dh-virtualenv` packaging, `.deb` built by CI on tag |
| `.github/workflows/` | `ci.yml` (tests on push/PR), `release.yml` (tag `v*` → build `.deb`) |

## Invariants (do not break)

- **Coverage stays at 100 %** — gate in `pyproject.toml` via
  `--cov-fail-under=100`. CI uses the marker
  `pytest -m "not requires_bchash"` for the two tests that need the real
  binary, and coverage still reaches 100 %.
- **FR/EN translations mirror each other** in `src/bc_vigil/i18n.py`.
- **No plugin system** — a new module is simply a sub-package under
  `src/bc_vigil/` with its own routes/templates/logic, wired in
  `app.py` via `include_router`.
- **DB timestamps stored in UTC**, presented via the `localtime` Jinja
  filter (reads `BC_VIGIL_DISPLAY_TZ`).
- **SQLite migrations**: nullable `ADD COLUMN` is handled automatically by
  `db._add_missing_columns` on startup. Anything else (drops, renames,
  NOT NULL with backfill) is a manual task.

## Adding a new module (summary)

1. Create `src/bc_vigil/<mod>/` with logic + `routes.py`
2. Add templates under `src/bc_vigil/templates/<mod>/`
3. Add i18n keys in **both** `fr` and `en` dicts of `i18n.py`
4. Wire in `src/bc_vigil/app.py`: import + `app.include_router(...)`
5. Add nav link in `src/bc_vigil/templates/base.html`
6. Add tests under `tests/` until coverage is back at 100 %
7. Update `help_fr.html` and `help_en.html` if user-facing

## Common commands

```bash
pytest                          # full suite, 100 % gate
pytest -m "not requires_bchash" # CI-equivalent run (no bc-hash needed)
bc-vigil --reload               # dev server on http://127.0.0.1:8080
dpkg-buildpackage -us -uc -b    # build .deb locally (Debian trixie host)
```

## Release cycle

1. Bump `pyproject.toml` **and** `debian/changelog` in lockstep
2. `git commit -am "Release X.Y.Z"`
3. `git tag vX.Y.Z && git push origin main vX.Y.Z`
4. `release.yml` builds the `.deb` in a `debian:trixie` container and
   attaches it to the GitHub Release
5. On each target server: `curl` the `.deb`, `sudo apt install ./it.deb`

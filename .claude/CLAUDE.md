# BitCrafts Vigil — project context

Web frontend (FastAPI + SQLite + APScheduler + Jinja2/HTMX) for the
`bc-hash` binary (requires `hash`, `check`, and `diff` subcommands in `$PATH`).
Lets operators register file-tree targets, schedule periodic hashes,
and detect integrity drift (`added` / `removed` / `modified`) against
a baseline.

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

# BitCrafts Vigil — project context

Web frontend (FastAPI + SQLite + APScheduler + Jinja2/HTMX) wrapping
two CLI tools:

- `bc-hash` (integrity module — file integrity monitoring / drift detection
  via baseline + `diff`).
- `bc-duplicate` (dedup module — duplicate group detection + safe move to
  quarantine with `copy+unlink` or `rename`).

Both share the same app, DB, i18n, sandboxed systemd unit.

## Invariants (do not break)

- **Coverage stays at 100 %** — gate in `pyproject.toml` via
  `--cov-fail-under=100`. CI uses the marker
  `pytest -m "not requires_bchash"` for the tests that need the real
  binary, and coverage still reaches 100 %.
- **FR/EN translations mirror each other** in `src/bc_vigil/i18n.py`.
  Enforced by `tests/test_i18n_mirror.py` — a missing key in one
  language fails CI.
- **No plugin system** — a new module is simply a sub-package under
  `src/bc_vigil/` with its own routes/templates/logic, wired in
  `app.py` via `include_router`.
- **DB timestamps stored in UTC**, presented via the `localtime` Jinja
  filter (reads `BC_VIGIL_DISPLAY_TZ`).
- **Cron schedules stored and evaluated in `BC_VIGIL_DISPLAY_TZ`**
  (APScheduler + `cron_builder.next_occurrences` both use that zone).
  The default for `BC_VIGIL_DISPLAY_TZ` is auto-detected from `$TZ` /
  `/etc/timezone` / `/etc/localtime` at startup, fallback UTC.
- **SQLite migrations**: nullable `ADD COLUMN` is handled automatically by
  `db._add_missing_columns` on startup. Anything else (drops, renames,
  NOT NULL with backfill) is a manual task. Alembic is on the roadmap.
- **Dedup trash is write-bounded to `/var/lib/bc-vigil` by default**. To
  dedup paths outside this directory (e.g. `/storage`), a systemd drop-in
  extending `ReadWritePaths` + adding `CAP_DAC_OVERRIDE` is required. See
  `packaging/README.md` "Dedup on paths outside /var/lib/bc-vigil".

## Adding a new module (summary)

1. Create `src/bc_vigil/<mod>/` with logic + `routes.py`
2. Add templates under `src/bc_vigil/templates/<mod>/`
3. Add i18n keys in **both** `fr` and `en` dicts of `i18n.py` (the
   mirror test will fail otherwise)
4. Wire in `src/bc_vigil/app.py`: import + `app.include_router(...)`
5. Add nav link in `src/bc_vigil/templates/base.html`
6. Add tests under `tests/` until coverage is back at 100 %
7. Update `help_fr.html` and `help_en.html` if user-facing

## Observability / health

- `GET /health` is the canonical readiness probe (JSON, 200 green /
  503 degraded). It covers DB reachability and both scheduler states.
- `GET /metrics` returns a Prometheus text exposition (no dependency
  on `prometheus_client`). Exposes `bc_vigil_up`, `bc_vigil_info`,
  `bc_vigil_db_up`, `bc_vigil_scheduler_up{module}`,
  `bc_vigil_scans_total{module, status}`,
  `bc_vigil_dedup_deletions_total{status}`.
- Monthly `VACUUM` job on SQLite (`0 4 1 * *` local), scheduled from
  `integrity/scheduler.py::_install_vacuum_job`.
- Stale scan cleanup at startup: `_cleanup_stale_scans` in both
  schedulers marks pending/running scans older than the current
  process start-time as `failed`. An INFO log line reports the count.

## Help pages (1.1.0+)

Help is served as per-topic pages at `/help/<topic>` where topic is
one of `overview`, `integrity`, `dedup`, `admin`, `faq`. `/help`
redirects (303) to `/help/overview`. Templates under
`templates/help/`: `page.html` is the parent with a sticky sidebar,
and `<topic>_<lang>.html` are fragment templates included based on
`current_topic` + `current_lang(request)`. Each new topic = one entry
in `core/routes/help.py::TOPICS` + 2 fragments (fr + en) + an
`help.topic.<slug>` i18n key.

## Editing existing targets / schedules (0.5.6+)

- `GET /targets/{id}/edit` + `POST /targets/{id}/update` — integrity
- `GET /dedup/targets/{id}/edit` + `POST /dedup/targets/{id}/update` — dedup
- `GET /schedules/{id}/edit` + `POST /schedules/{id}/update` — integrity
- `GET /dedup/schedules/{id}/edit` + `POST /dedup/schedules/{id}/update` — dedup

All preserve the row id, history, baseline link, and related schedules.
`path` is immutable on targets (rendered `readonly` in the form): a
different path is conceptually a different target.

## Retry failed dedup deletions (0.5.6+)

`POST /dedup/trash/{id}/retry` re-attempts the move for a trash row in
`status=failed`. On success, the original failed row is dropped and
replaced by the new quarantined row (keeps the trash view clean). On
failure, the original row is updated with the new error and timestamp;
no duplicate row is created.

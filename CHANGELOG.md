# Changelog

User-facing highlights per release. See `debian/changelog` for the full
per-release detail.

## 1.2.1 — 2026-04-21

Help pages refresh.

- Help content refreshed to document features shipped between 0.5.3
  and 1.2.0 that had not yet made it into user-facing help:
  integrity & dedup target Edit button (0.5.6), schedule Edit (0.5.6),
  retry failed dedup deletion (0.5.6), `/health` and `/metrics`
  endpoints (0.5.7 / 1.2.0), stale scan cleanup at startup (0.5.4),
  monthly VACUUM (0.5.7), and the `BC_VIGIL_DISPLAY_TZ` auto-detect
  default (0.5.5). FR + EN.

## 1.2.0 — 2026-04-21

Prometheus exposition.

- New `GET /metrics` endpoint in the Prometheus text exposition format.
  Exposes `bc_vigil_up`, `bc_vigil_info{version}`, `bc_vigil_db_up`,
  `bc_vigil_scheduler_up{module}`, `bc_vigil_scans_total{module, status}`,
  `bc_vigil_dedup_deletions_total{status}`. No new dependency (no
  `prometheus_client`) — the exposition is produced manually.
- Intended for scraping every 30s alongside `/health` JSON probes.
  Tested in production with a scrape from an existing Prometheus +
  Grafana stack.

## 1.1.0 — 2026-04-21

Help pages split into per-topic pages with a sticky sidebar.

- `/help` redirects (303) to `/help/overview`. Each of the five topics
  (`overview`, `integrity`, `dedup`, `admin`, `faq`) now has its own
  URL `/help/<slug>`.
- New parent template `templates/help/page.html` with a left sticky
  sidebar nav + `{% include %}` of the selected fragment.
- 10 fragment templates under `templates/help/<topic>_<lang>.html`
  (5 topics × 2 languages).
- i18n: `help.nav.title`, `help.topic.<slug>` added FR + EN.
- Old monolithic `help_fr.html` / `help_en.html` removed.
- Audit: no TODO/FIXME/XXX/HACK in `src/`.

## 1.0.0 — 2026-04-21

First "complete" release. Everything advertised in the feature list
works, the dedup module is production-usable, the UI no longer has
known blocking bugs, observability basics are in place.

Highlights accumulated over the 0.5.x series:

### Functional

- **Integrity module** (0.x → 1.0): register targets, schedule scans,
  detect `added` / `removed` / `modified` drift against a baseline.
  Baseline auto-promotion or manual promote from any successful scan.
  FR / EN UI.
- **Dedup module** (0.5.x): scan a target, review duplicate groups,
  auto-resolve rules, manual selection preview, execute a move-to-
  trash plan, restore or purge individual trash entries. Handles cross-
  FS moves (`copy+unlink` vs `rename`) transparently.
- **Editing targets and schedules** (0.5.6): full round-trip edit with
  history preservation (baseline_scan_id, last_scan_id, scan list,
  schedule id). Immutable `path` on targets.
- **Retry failed dedup deletions** (0.5.6): POST
  `/dedup/trash/{id}/retry` re-attempts the move for rows in
  `status=failed`. Typical use: recovers from an EROFS at the first
  attempt (e.g. systemd `ProtectSystem` drop-in was missing).

### UI / UX

- **24h time picker** (0.5.4): replaced `<input type="time">` with a
  paired `<select hour><select minute>` + hidden input, immune to
  browser locale AM/PM.
- **Page noire fix** (0.5.5): live scan pages no longer blank out
  between auto-refresh ticks (`hx-target="this"/"main"` instead of
  `"body"`).
- **Nav disambiguation** (0.5.5): "Cibles intégrité" / "Cibles
  doublons" / "Scans intégrité" / "Scans doublons" (FR) and
  corresponding EN labels.
- **Favicon** (0.5.5).
- **Confirm labels reworded** (0.5.4): "Taper <code>X</code> pour
  confirmer" instead of "Confirm (type <code>X</code>)" to avoid the
  awkward line-wrap inside parentheses.

### Platform

- **Timezone correctness** (0.5.4): cron schedules are stored and
  evaluated in `BC_VIGIL_DISPLAY_TZ`, not hard-coded UTC. Default
  auto-detected from system (`$TZ` / `/etc/timezone` / `/etc/localtime`
  symlink, fallback UTC).
- **Stale scan cleanup** (0.5.4): scans left in `pending` / `running`
  by a previous dead bc-vigil process are auto-marked `failed` at
  startup. Uses process start-time so in-flight scans of the current
  process are untouched.
- **Empty dedup target = success** (0.5.3): running a dedup scan on an
  empty volume is no longer flagged as `failed`; bc-duplicate omits
  its JSON output in that case, which is now treated as an empty
  successful scan.

### Observability

- **Health endpoint** (0.5.7): `GET /health` returns JSON with service
  status, version, DB reachability, and the state of both schedulers.
  `200` green, `503` degraded.
- **Startup log line** (0.5.5): an `INFO` entry reports the count of
  stale scans cleaned.
- **SQLite VACUUM** (0.5.7): monthly job (`0 4 1 * *` local)
  reclaims space after retention purge.

### Testing

- **100 % coverage gate** maintained throughout (~450 tests).
- **FR/EN i18n mirror invariant** enforced by a dedicated test
  (`tests/test_i18n_mirror.py`).
- **UI regression suite** (`tests/test_ui_regressions.py`) guards
  against reintroducing the 0.5.4 / 0.5.5 bugs.

## License

MIT — see [LICENSE](LICENSE).

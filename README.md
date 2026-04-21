# BitCrafts Vigil

> **Scope.** Personal project, designed for use on a trusted **intranet / LAN**
> only. It has no authentication, no TLS, and no rate limiting — do **not**
> expose it to the public Internet. Published here for transparency and reuse,
> not as a hardened product.
>
> **Support.** Issues and PRs are welcome but handled on a best-effort basis,
> whenever I have spare time — this is not a priority project and there is no
> SLA. Do not rely on a timely response.

Web frontend wrapping two CLIs:

- [`bc-hash`](https://github.com/unmanaged-bytes/bc-hash) — **file integrity
  monitoring**: baseline, drift detection (`added` / `removed` / `modified`),
  scheduled scans.
- [`bc-duplicate`](https://github.com/unmanaged-bytes/bc-duplicate) — **dedup
  scanning**: detect duplicate groups, plan + execute safe moves to quarantine
  (trash with `copy+unlink` or `rename` depending on FS boundary), restore or
  purge.

Both modules share the same web UI (FastAPI + HTMX + SQLite), FR/EN i18n,
sandboxed systemd service, 100 % test coverage gate.

## Requirements

- Python 3.11+
- `bc-hash` (>= 1.0.0, with the `diff` subcommand) on `PATH`
- `bc-duplicate` (>= 0.1.0) on `PATH`

## Install (Debian 13 trixie — production)

Download the latest `.deb` from the GitHub Releases page, then:

```bash
sudo apt install ./bc-vigil_X.Y.Z-1_amd64.deb
systemctl status bc-vigil
curl http://127.0.0.1:8080/health   # returns JSON status
```

The package:
- Installs an embedded virtualenv under `/opt/venvs/bc-vigil/` (offline-friendly)
- Creates a `bc-vigil` system user
- Installs a sandboxed systemd unit (`ProtectSystem=strict`,
  `ReadWritePaths=/var/lib/bc-vigil`, `CAP_DAC_READ_SEARCH`), enables and
  starts it
- Listens on `0.0.0.0:8080` by default (tunable via
  `/etc/bc-vigil/bc-vigil.env`)

See [packaging/README.md](packaging/README.md) for the full deploy / upgrade /
rollback procedure, and in particular the **systemd drop-in** required for
dedup on paths outside `/var/lib/bc-vigil` (e.g. `/storage/...`).

## Development

```bash
git clone https://github.com/unmanaged-bytes/bc-vigil.git
cd bc-vigil
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

pytest                      # full test suite, 100 % coverage gate
bc-vigil --reload           # dev server on http://127.0.0.1:8080
```

## Configuration

Environment variables (all prefixed with `BC_VIGIL_`):

| Variable | Default | Purpose |
|---|---|---|
| `BC_VIGIL_HOST` | `127.0.0.1` | Listen address (`.deb` ships with `0.0.0.0`) |
| `BC_VIGIL_PORT` | `8080` | Listen port |
| `BC_VIGIL_DATA_DIR` | `./var` | SQLite DB + digests directory |
| `BC_VIGIL_BC_HASH_BINARY` | `bc-hash` | Path or name of the `bc-hash` binary |
| `BC_VIGIL_BC_DUPLICATE_BINARY` | `bc-duplicate` | Path or name of the `bc-duplicate` binary |
| `BC_VIGIL_DEFAULT_ALGORITHM` | `sha256` | `crc32`, `sha256`, `xxh3`, `xxh128` |
| `BC_VIGIL_DEFAULT_THREADS` | `auto` | `auto`, `0`, or an integer |
| `BC_VIGIL_MAX_PARALLEL_SCANS` | `2` | Max concurrent scans |
| `BC_VIGIL_DISPLAY_TZ` | auto-detect | Read from `$TZ`, then `/etc/timezone`, then `/etc/localtime` symlink, fallback `UTC`. Cron schedules are both stored and evaluated in this zone. |
| `BC_VIGIL_SCAN_RETENTION_DAYS` | `0` | Auto-purge finished scans older than N days (0 = off) |
| `BC_VIGIL_DEDUP_TRASH_DIR` | *(unset)* | Override trash location. Default: `<data_dir>/dedup/trash`. |
| `BC_VIGIL_DEDUP_TRASH_RETENTION_DAYS` | `7` | Auto-purge trash entries older than N days |
| `BC_VIGIL_DEDUP_DELETION_BULK_THRESHOLD` | `500` | Above this count the UI requires explicit opt-in before executing a delete plan |

## Data model

### Integrity module

- **Target** — name + path + algorithm + threads + include/exclude globs
- **Schedule** — cron expression attached to a target (evaluated in `BC_VIGIL_DISPLAY_TZ`)
- **Scan** — run: `bc-hash hash` produces a digest, `bc-hash diff` compares to baseline
- **Baseline** — first successful scan is promoted automatically; "Promote baseline" button on any successful scan to adopt a new reference
- **IntegrityEvent** — one row per changed file: `added` / `removed` / `modified`

### Dedup module

- **DedupTarget** — path + algorithm + threads + filters (minimum_size, one_file_system, etc.)
- **DedupSchedule** — cron (same semantics as integrity schedules)
- **DedupScan** — run: `bc-duplicate scan` produces a group/file JSON; bc-vigil stores groups for UI review
- **DedupGroup** — a set of identical-content files detected by a scan
- **DedupDeletion** — a file moved to trash via `copy+unlink` or `rename`, with status `quarantined` / `restored` / `purged` / `failed`

## Architecture

```
src/bc_vigil/
├── app.py, config.py, db.py, models.py, i18n.py      # infra
├── core/          # dashboard, admin (backup/restore/reset), help, lang switch
├── integrity/     # bc-hash orchestration (targets, scans, schedules)
├── dedup/         # bc-duplicate orchestration + quarantine (trash)
└── storage/       # disk usage module (psutil)
```

Templates under `src/bc_vigil/templates/`. FR and EN strings in `i18n.py`,
switched via cookie + `/lang/<code>` route. Mirror invariant enforced by
`tests/test_i18n_mirror.py`.

## Observability

- `GET /health` returns JSON with service status, DB reachability, and the
  state of both schedulers (integrity + dedup). Returns `200` when green,
  `503` when degraded.
- On startup, stale scans (pending/running from a previous dead process)
  are auto-marked as failed and an `INFO` log line reports the count.
- Monthly `VACUUM` job keeps the SQLite DB tight after `scan_retention_days`
  purges.

## License

MIT — see [LICENSE](LICENSE).

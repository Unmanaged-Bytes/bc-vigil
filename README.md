# BitCrafts Vigil

> **Scope.** Personal project, designed for use on a trusted **intranet / LAN**
> only. It has no authentication, no TLS, and no rate limiting — do **not**
> expose it to the public Internet. Published here for transparency and reuse,
> not as a hardened product.
>
> **Support.** Issues and PRs are welcome but handled on a best-effort basis,
> whenever I have spare time — this is not a priority project and there is no
> SLA. Do not rely on a timely response.

Web frontend for [`bc-hash`](https://github.com/unmanaged-bytes/bc-hash). Register file trees to
monitor, schedule periodic scans, and detect integrity drift
(`added` / `removed` / `modified`) against a reference baseline.

Also ships a **Storage** module showing per-mountpoint disk usage, DB
backup/restore/reset, FR/EN UI, and a 100 % test coverage gate.

## Requirements

- Python 3.11+
- `bc-hash` (>= 1.0.0, with the `diff` subcommand) available in `PATH`

## Install (Debian 13 trixie — production)

Download the latest `.deb` from the GitHub Releases page, then:

```bash
sudo apt install ./bc-vigil_X.Y.Z-1_amd64.deb
systemctl status bc-vigil
curl http://127.0.0.1:8080/
```

The package:
- Installs an embedded virtualenv under `/opt/venvs/bc-vigil/` (offline-friendly)
- Creates a `bc-vigil` system user
- Installs a sandboxed systemd unit, enables and starts it
- Listens on `0.0.0.0:8080` by default (tunable via `/etc/bc-vigil/bc-vigil.env`)

See [packaging/README.md](packaging/README.md) for the full deploy / upgrade /
rollback procedure.

## Development

```bash
git clone https://github.com/unmanaged-bytes/bc-vigil.git
cd bc-vigil
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

pytest                      # full test suite (190 tests, 100 % coverage gate)
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
| `BC_VIGIL_DEFAULT_ALGORITHM` | `sha256` | `crc32`, `sha256`, `xxh3`, `xxh128` |
| `BC_VIGIL_DEFAULT_THREADS` | `auto` | `auto`, `0`, or an integer |
| `BC_VIGIL_MAX_PARALLEL_SCANS` | `2` | Max concurrent scans |
| `BC_VIGIL_DISPLAY_TZ` | `UTC` | Display timezone (e.g. `Europe/Paris`) — DB stays UTC |
| `BC_VIGIL_SCAN_RETENTION_DAYS` | `0` | Auto-purge finished scans older than N days (0 = off) |

## Data model

- **Target** — name + path + algorithm + threads + include/exclude globs
- **Schedule** — UTC cron expression attached to a target
- **Scan** — run: `bc-hash hash` produces a digest, `bc-hash diff` compares to baseline
- **Baseline** — first successful scan becomes baseline automatically;
  "Promote baseline" button on any successful scan to adopt a new reference
- **IntegrityEvent** — one row per changed file: `added` / `removed` / `modified`

## Architecture

```
src/bc_vigil/
├── app.py, config.py, db.py, models.py, i18n.py   # infra
├── core/          # dashboard, admin (backup/restore/reset), help, lang switch
├── integrity/     # bc-hash orchestration: targets, scans, schedules
└── storage/       # disk usage module (psutil)
```

Templates under `src/bc_vigil/templates/` (FR and EN strings in `i18n.py`,
switched via cookie + `/lang/<code>` route).

## License

MIT — see [LICENSE](LICENSE).

# Packaging & deployment

## Workflow: CI/CD via GitHub Actions

### `.github/workflows/ci.yml`

On every push to `main` and every PR:
- Installs deps and runs `pytest`
- Enforces the **100 %** coverage gate (configured in `pyproject.toml`)
- Skips tests that require the real `bc-hash` binary (marker `requires_bchash`)

### `.github/workflows/release.yml`

On every pushed `v*` tag:
- Verifies that `debian/changelog` matches the tag version
- Builds the `.deb` in a `debian:trixie` container (same base as production
  servers — guarantees glibc / psutil compatibility)
- Attaches `bc-vigil_X.Y.Z-1_amd64.deb`, `.buildinfo`, and `.changes` to the
  GitHub Release
- Auto-generates release notes from commits

## Cutting a release

```bash
# 1. Bump version in lockstep: pyproject.toml + debian/changelog
dch -v 0.2.0-1 "Release 0.2.0"   # interactive, or edit by hand

# 2. Commit
git commit -am "Release 0.2.0"

# 3. Tag and push
git tag v0.2.0
git push origin main v0.2.0

# 4. Wait for CI — .deb will appear on the GitHub Release page
```

## Deploying to a server

No Ansible, no auto-push. On each target server, when you choose to:

```bash
ssh root-admin@<server>

VER=$(curl -s https://api.github.com/repos/unmanaged-bytes/bc-vigil/releases/latest \
      | grep -oP '"tag_name":\s*"\K[^"]+')
DEB="bc-vigil_${VER#v}-1_amd64.deb"

curl -sL "https://github.com/unmanaged-bytes/bc-vigil/releases/download/${VER}/${DEB}" \
     -o "/tmp/${DEB}"
sudo apt install -y "/tmp/${DEB}"
rm "/tmp/${DEB}"

systemctl status bc-vigil
curl http://127.0.0.1:8080/
```

### UFW — one-time step per server

The service listens on `0.0.0.0:8080`. If the server has UFW enabled with a
default `deny incoming` policy, allow access from your LAN:

```bash
sudo ufw allow from 192.168.0.0/16 to any port 8080 proto tcp
```

Adjust the source CIDR to match your own network.

## What the `.deb` contains

| Path | Purpose |
|---|---|
| `/opt/venvs/bc-vigil/` | Embedded venv + source (offline-installable) |
| `/lib/systemd/system/bc-vigil.service` | Sandboxed systemd unit |
| `/etc/bc-vigil/bc-vigil.env` | Conffile (preserved across upgrades) |
| `/var/lib/bc-vigil/` | State directory, owned by `bc-vigil:bc-vigil` |

Default listen: `0.0.0.0:8080`. Override via `/etc/bc-vigil/bc-vigil.env`.

## Upgrading

Running `apt install ./bc-vigil_X.Y.Z-1_amd64.deb` does an in-place upgrade:

- Service is stopped, new code installed, service restarted (via the
  `#DEBHELPER#` hooks in postinst/prerm)
- The `bc-vigil.env` conffile is preserved (dpkg handles it as a conffile)
- The SQLite DB is auto-migrated on startup (`_add_missing_columns` in
  `src/bc_vigil/db.py` — handles nullable `ALTER TABLE ADD COLUMN`)

**Rollback**: `apt install ./bc-vigil_PREV_VERSION_amd64.deb` (keep previous
`.deb` files around if you need this).

## Uninstall

```bash
sudo apt remove bc-vigil      # keeps /var/lib/bc-vigil (data preserved)
sudo apt purge bc-vigil       # removes user, data dir, and /etc/bc-vigil
```

## Local build (for troubleshooting)

Prerequisites on a Debian trixie workstation:

```bash
sudo apt install debhelper dh-virtualenv python3-dev python3-venv \
                 build-essential devscripts
```

Build:

```bash
dpkg-buildpackage -us -uc -b
# artefact appears in the parent directory: ../bc-vigil_X.Y.Z-1_amd64.deb
```

Useful to test packaging before tagging, or if GitHub Actions is unavailable.

## `debian/` layout

```
debian/
├── changelog            # versions + dates (RFC 5322) — must match the git tag
├── control              # build and runtime dependencies
├── copyright            # MIT
├── rules                # Makefile driving dh-virtualenv
├── compat               # debhelper compatibility level (13)
├── source/format        # 3.0 (native)
├── bc-vigil.install     # copies bc-vigil.env into /etc/bc-vigil/
├── bc-vigil.conffiles   # list of conffiles preserved across upgrades
├── bc-vigil.service     # systemd unit (dh_installsystemd handles it)
├── bc-vigil.env         # default env file (conffile)
├── bc-vigil.postinst    # useradd, chown state dir, warn if bc-hash missing
├── bc-vigil.prerm       # #DEBHELPER# hook (stops service on removal)
└── bc-vigil.postrm      # removes user + /var/lib/bc-vigil on purge
```

from __future__ import annotations

import json
import re
import shutil
import signal
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path

from bc_vigil.config import settings


class BcHashError(RuntimeError):
    pass


class BcHashCancelled(BcHashError):
    pass


class CancelHandle:
    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._cancelled = False
        self._forced = False
        self._lock = threading.Lock()

    def attach(self, proc: subprocess.Popen) -> None:
        with self._lock:
            self._proc = proc
            if self._cancelled:
                self._signal_locked()

    def cancel(self, force: bool = False) -> bool:
        with self._lock:
            self._cancelled = True
            if force:
                self._forced = True
            return self._signal_locked()

    def _signal_locked(self) -> bool:
        if self._proc is None or self._proc.poll() is not None:
            return False
        sig = signal.SIGKILL if self._forced else signal.SIGTERM
        try:
            self._proc.send_signal(sig)
            return True
        except ProcessLookupError:
            return False

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled


@dataclass
class HashResult:
    digest_path: Path
    files_total: int
    bytes_total: int
    wall_ms: int
    files_error: int
    peak_rss_bytes: int | None = None


class _RssSampler:
    def __init__(self, pid: int, interval: float = 0.05) -> None:
        self._pid = pid
        self._interval = interval
        self._peak_kb = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> int | None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        self._sample_once()
        return self._peak_kb * 1024 if self._peak_kb else None

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample_once()
            if self._stop.wait(self._interval):
                return

    def _sample_once(self) -> None:
        try:
            with open(f"/proc/{self._pid}/status") as fh:
                for line in fh:
                    if line.startswith("VmHWM:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            val = int(parts[1])
                            if val > self._peak_kb:
                                self._peak_kb = val
                        return
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            return


@dataclass
class DiffEvent:
    event_type: str
    path: str
    old_digest: str | None = None
    new_digest: str | None = None


@dataclass
class DiffResult:
    added: int
    removed: int
    modified: int
    unchanged: int
    events: list[DiffEvent]


_MODIFIED_RE = re.compile(
    r"^MODIFIED\s+(?P<path>.+?)\s+(?P<old>[0-9a-f]+)\s*->\s*(?P<new>[0-9a-f]+)\s*$"
)
_ADDED_RE = re.compile(r"^ADDED\s+(?P<path>.+?)\s*$")
_REMOVED_RE = re.compile(r"^REMOVED\s+(?P<path>.+?)\s*$")
_SUMMARY_RE = re.compile(
    r"(?P<added>\d+)\s+added,\s+(?P<removed>\d+)\s+removed,\s+"
    r"(?P<modified>\d+)\s+modified,\s+(?P<unchanged>\d+)\s+unchanged"
)


def _binary() -> str:
    resolved = shutil.which(settings.bc_hash_binary)
    if resolved is None:
        raise BcHashError(f"bc-hash binary not found: {settings.bc_hash_binary}")
    return resolved


def parse_patterns(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [line.strip() for line in raw.splitlines() if line.strip()]


def run_hash(
    target_path: Path,
    digest_path: Path,
    algorithm: str,
    threads: str = "auto",
    includes: list[str] | None = None,
    excludes: list[str] | None = None,
    cancel: CancelHandle | None = None,
) -> HashResult:
    if not target_path.exists():
        raise BcHashError(f"target path does not exist: {target_path}")

    digest_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        _binary(),
        f"--threads={threads}",
        "hash",
        f"--type={algorithm}",
        f"--output={digest_path}",
    ]
    for pattern in includes or ():
        cmd.append(f"--include={pattern}")
    for pattern in excludes or ():
        cmd.append(f"--exclude={pattern}")
    cmd.append(str(target_path))
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if cancel is not None:
        cancel.attach(proc)
    sampler = _RssSampler(proc.pid)
    sampler.start()
    try:
        stdout, stderr = proc.communicate()
    finally:
        peak_rss = sampler.stop()

    if cancel is not None and cancel.cancelled:
        try:
            digest_path.unlink()
        except FileNotFoundError:
            pass
        raise BcHashCancelled("scan cancelled")

    if proc.returncode != 0:
        raise BcHashError(
            f"bc-hash hash failed (exit {proc.returncode}): "
            f"{stderr.strip() or stdout.strip()}"
        )

    if not digest_path.exists():
        digest_path.write_text(json.dumps({
            "type": "summary",
            "files_total": 0,
            "bytes_total": 0,
            "wall_ms": 0,
            "files_error": 0,
        }) + "\n")

    summary = _read_summary(digest_path)
    return HashResult(
        digest_path=digest_path,
        files_total=summary["files_total"],
        bytes_total=summary["bytes_total"],
        wall_ms=summary["wall_ms"],
        files_error=summary.get("files_error", 0),
        peak_rss_bytes=peak_rss,
    )


def run_diff(old_digest: Path, new_digest: Path) -> DiffResult:
    cmd = [_binary(), "diff", str(old_digest), str(new_digest)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode not in (0, 1):
        raise BcHashError(
            f"bc-hash diff failed (exit {proc.returncode}): "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )

    events: list[DiffEvent] = []
    for line in proc.stdout.splitlines():
        if not line or line.startswith("bc-hash:"):
            continue
        if m := _MODIFIED_RE.match(line):
            events.append(DiffEvent("modified", m["path"], m["old"], m["new"]))
        elif m := _ADDED_RE.match(line):
            events.append(DiffEvent("added", m["path"]))
        elif m := _REMOVED_RE.match(line):
            events.append(DiffEvent("removed", m["path"]))

    summary_line = next(
        (ln for ln in proc.stderr.splitlines() + proc.stdout.splitlines()
         if _SUMMARY_RE.search(ln)),
        "",
    )
    m = _SUMMARY_RE.search(summary_line)
    if not m:
        return DiffResult(
            added=sum(1 for e in events if e.event_type == "added"),
            removed=sum(1 for e in events if e.event_type == "removed"),
            modified=sum(1 for e in events if e.event_type == "modified"),
            unchanged=0,
            events=events,
        )
    return DiffResult(
        added=int(m["added"]),
        removed=int(m["removed"]),
        modified=int(m["modified"]),
        unchanged=int(m["unchanged"]),
        events=events,
    )


def _read_summary(digest_path: Path) -> dict:
    summary: dict | None = None
    with digest_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("type") == "summary":
                summary = record
    if summary is None:
        raise BcHashError(f"no summary record in digest: {digest_path}")
    return summary

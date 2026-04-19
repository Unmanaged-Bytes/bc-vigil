from __future__ import annotations

import json
import re
import shutil
import signal
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path

from bc_vigil.config import settings


class BcDuplicateError(RuntimeError):
    pass


class BcDuplicateCancelled(BcDuplicateError):
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


class _RssSampler:
    def __init__(self, pid: int, interval: float = 0.2) -> None:
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
class DuplicateGroup:
    size: int
    files: list[str]


@dataclass
class ScanResult:
    groups: list[DuplicateGroup] = field(default_factory=list)
    duplicate_groups: int = 0
    duplicate_files: int = 0
    wasted_bytes: int = 0
    wall_ms: int = 0
    files_scanned: int = 0
    directories_scanned: int = 0
    files_skipped: int = 0
    hardlinks_collapsed: int = 0
    size_candidates: int = 0
    files_hashed_fast: int = 0
    files_hashed_full: int = 0
    algorithm: str | None = None
    peak_rss_bytes: int | None = None


_STATS_RE = re.compile(
    r"^bc-duplicate:\s+(?P<groups>\d+)\s+duplicate group\(s\),\s+"
    r"(?P<files>\d+)\s+duplicate file\(s\),\s+"
    r"(?P<bytes>\d+)\s+wasted byte\(s\)\s+in\s+(?P<ms>\d+)\s+ms\s*$"
)


def _binary() -> str:
    resolved = shutil.which(settings.bc_duplicate_binary)
    if resolved is None:
        raise BcDuplicateError(
            f"bc-duplicate binary not found: {settings.bc_duplicate_binary}"
        )
    return resolved


def parse_patterns(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [line.strip() for line in raw.splitlines() if line.strip()]


def run_scan(
    target_path: Path,
    output_path: Path,
    algorithm: str = "xxh3",
    threads: str = "auto",
    includes: list[str] | None = None,
    excludes: list[str] | None = None,
    minimum_size: int | None = None,
    include_hidden: bool = False,
    follow_symlinks: bool = False,
    match_hardlinks: bool = False,
    one_file_system: bool = False,
    cancel: CancelHandle | None = None,
) -> ScanResult:
    if not target_path.exists():
        raise BcDuplicateError(f"target path does not exist: {target_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        _binary(),
        f"--threads={threads}",
        "scan",
        f"--algorithm={algorithm}",
        f"--output={output_path}",
    ]
    if minimum_size is not None:
        cmd.append(f"--minimum-size={int(minimum_size)}")
    for pattern in includes or ():
        cmd.append(f"--include={pattern}")
    for pattern in excludes or ():
        cmd.append(f"--exclude={pattern}")
    if include_hidden:
        cmd.append("--hidden")
    if follow_symlinks:
        cmd.append("--follow-symlinks")
    if match_hardlinks:
        cmd.append("--match-hardlinks")
    if one_file_system:
        cmd.append("--one-file-system")
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
            output_path.unlink()
        except FileNotFoundError:
            pass
        raise BcDuplicateCancelled("scan cancelled")

    if proc.returncode != 0:
        raise BcDuplicateError(
            f"bc-duplicate scan failed (exit {proc.returncode}): "
            f"{stderr.strip() or stdout.strip()}"
        )

    stats_from_stderr = _parse_stats_line(stderr)
    result = _parse_output_json(output_path)
    if stats_from_stderr is not None:
        (
            result.duplicate_groups,
            result.duplicate_files,
            result.wasted_bytes,
            result.wall_ms,
        ) = stats_from_stderr
    result.peak_rss_bytes = peak_rss
    return result


def _parse_stats_line(stderr: str) -> tuple[int, int, int, int] | None:
    for line in stderr.splitlines():
        m = _STATS_RE.match(line.strip())
        if m:
            return (
                int(m["groups"]),
                int(m["files"]),
                int(m["bytes"]),
                int(m["ms"]),
            )
    return None


def _parse_output_json(output_path: Path) -> ScanResult:
    if not output_path.exists():
        raise BcDuplicateError(f"bc-duplicate output missing: {output_path}")
    try:
        raw = output_path.read_text()
        payload = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise BcDuplicateError(
            f"bc-duplicate output invalid at {output_path}: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise BcDuplicateError(
            f"bc-duplicate output is not a JSON object at {output_path}"
        )

    stats = payload.get("stats") or {}
    groups_raw = payload.get("groups") or []
    groups: list[DuplicateGroup] = []
    for entry in groups_raw:
        if not isinstance(entry, dict):
            continue
        files = entry.get("files") or []
        if not isinstance(files, list):
            continue
        size = int(entry.get("size") or 0)
        groups.append(DuplicateGroup(
            size=size,
            files=[str(p) for p in files if isinstance(p, str)],
        ))

    return ScanResult(
        groups=groups,
        duplicate_groups=int(stats.get("duplicate_groups") or 0),
        duplicate_files=int(stats.get("duplicate_files") or 0),
        wasted_bytes=int(stats.get("wasted_bytes") or 0),
        wall_ms=int(stats.get("wall_ms") or 0),
        files_scanned=int(stats.get("files_scanned") or 0),
        directories_scanned=int(stats.get("directories_scanned") or 0),
        files_skipped=int(stats.get("files_skipped") or 0),
        hardlinks_collapsed=int(stats.get("hardlinks_collapsed") or 0),
        size_candidates=int(stats.get("size_candidates") or 0),
        files_hashed_fast=int(stats.get("files_hashed_fast") or 0),
        files_hashed_full=int(stats.get("files_hashed_full") or 0),
        algorithm=payload.get("algorithm"),
    )

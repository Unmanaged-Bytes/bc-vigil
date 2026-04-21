import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


_ETC_TIMEZONE = Path("/etc/timezone")
_ETC_LOCALTIME = Path("/etc/localtime")


def _detect_system_tz() -> str:
    tz = os.environ.get("TZ")
    if tz:
        return tz
    if _ETC_TIMEZONE.is_file():
        name = _ETC_TIMEZONE.read_text().strip()
        if name:
            return name
    if _ETC_LOCALTIME.is_symlink():
        target = str(_ETC_LOCALTIME.readlink())
        if "zoneinfo/" in target:
            return target.split("zoneinfo/", 1)[1]
    return "UTC"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BC_VIGIL_", env_file=".env")

    host: str = "127.0.0.1"
    port: int = 8080
    data_dir: Path = Path("./var")
    bc_hash_binary: str = "bc-hash"
    bc_duplicate_binary: str = "bc-duplicate"
    default_algorithm: str = "sha256"
    default_threads: str = "auto"
    max_parallel_scans: int = 2
    display_tz: str = Field(default_factory=_detect_system_tz)
    scan_retention_days: int = 0
    dedup_trash_dir: Optional[Path] = None
    dedup_trash_retention_days: int = 7
    dedup_deletion_bulk_threshold: int = 500

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.data_dir / 'bc-vigil.sqlite'}"

    @property
    def digests_dir(self) -> Path:
        return self.data_dir / "digests"

    @property
    def dedup_dir(self) -> Path:
        return self.data_dir / "dedup"

    @property
    def dedup_trash_dir_resolved(self) -> Path:
        return self.dedup_trash_dir or (self.data_dir / "dedup" / "trash")


settings = Settings()

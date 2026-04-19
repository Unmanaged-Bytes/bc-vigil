from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    display_tz: str = "UTC"
    scan_retention_days: int = 0

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.data_dir / 'bc-vigil.sqlite'}"

    @property
    def digests_dir(self) -> Path:
        return self.data_dir / "digests"

    @property
    def dedup_dir(self) -> Path:
        return self.data_dir / "dedup"


settings = Settings()

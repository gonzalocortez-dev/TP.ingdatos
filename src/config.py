"""Configuración centralizada del proyecto.

Los valores se leen de variables de entorno (archivo ``.env`` opcional)
para que el mismo código corra en local o en otro entorno sin cambios.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    """Parámetros operativos del lake y de la API."""

    lake_root: Path
    api_base_url: str
    http_timeout_seconds: int
    http_max_retries: int
    stream_interval_seconds: int
    log_level: str

    @property
    def bronze_path(self) -> Path:
        return self.lake_root / "bronze" / "quotes"

    @property
    def silver_path(self) -> Path:
        return self.lake_root / "silver" / "quotes"

    @property
    def gold_path(self) -> Path:
        return self.lake_root / "gold"


def get_settings() -> Settings:
    """Construye la configuración a partir del entorno."""
    return Settings(
        lake_root=Path(os.getenv("LAKE_ROOT", "data/lake")),
        api_base_url=os.getenv("DOLAR_API_BASE_URL", "https://dolarapi.com").rstrip("/"),
        http_timeout_seconds=_as_int("HTTP_TIMEOUT_SECONDS", 15),
        http_max_retries=_as_int("HTTP_MAX_RETRIES", 3),
        stream_interval_seconds=_as_int("STREAM_INTERVAL_SECONDS", 30),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )

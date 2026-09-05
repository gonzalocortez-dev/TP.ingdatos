"""Cliente HTTP para DolarAPI (https://dolarapi.com/docs/).

La API es pública y no requiere autenticación. Este cliente unifica
varios endpoints en un único contrato de ingestión para la capa Bronze.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Fuentes que se consolidan en el lake. Cada item se etiqueta con `source`
# de la misma forma que el script original etiquetaba `platform_name`.
API_SOURCES: tuple[tuple[str, str], ...] = (
    ("dolares", "/v1/dolares"),
    ("cotizaciones", "/v1/cotizaciones"),
)


@dataclass(frozen=True)
class RawQuote:
    """Cotización cruda tal como llega de la API, más metadatos de ingestión."""

    source: str
    endpoint: str
    payload: dict[str, Any]


class DolarApiClient:
    """Obtiene cotizaciones de DolarAPI con reintentos y timeout."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "cotizaciones-medallion/1.0",
            }
        )

    def fetch_endpoint(self, path: str) -> list[dict[str, Any]]:
        """GET a un path de la API y normaliza la respuesta a lista de dicts.

        Args:
            path: Ruta relativa, por ejemplo ``/v1/dolares``.

        Returns:
            Lista de cotizaciones. Si el endpoint devuelve un objeto único,
            se envuelve en una lista para unificar el procesamiento.
        """
        url = f"{self.settings.api_base_url}{path}"
        last_error: Exception | None = None

        for attempt in range(1, self.settings.http_max_retries + 1):
            try:
                response = self.session.get(
                    url, timeout=self.settings.http_timeout_seconds
                )
                response.raise_for_status()
                payload = response.json()
                return self._as_list(payload)
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                wait_seconds = 2 ** (attempt - 1)
                logger.warning(
                    "Fallo al consultar %s (intento %s/%s): %s. Reintento en %ss",
                    url,
                    attempt,
                    self.settings.http_max_retries,
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"No se pudo obtener {url}") from last_error

    def fetch_all(self) -> list[RawQuote]:
        """Descarga todas las fuentes configuradas y las etiqueta.

        Returns:
            Lista de cotizaciones crudas listas para persistir en Bronze.
        """
        quotes: list[RawQuote] = []
        for source, path in API_SOURCES:
            records = self.fetch_endpoint(path)
            logger.info("Fuente %s: %s registros", source, len(records))
            quotes.extend(
                RawQuote(source=source, endpoint=path, payload=record)
                for record in records
            )
        return quotes

    @staticmethod
    def _as_list(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        raise ValueError(f"Respuesta inesperada de la API: {type(payload)!r}")

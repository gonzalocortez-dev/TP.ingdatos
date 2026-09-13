"""Extracción desde DolarAPI: modo FULL e INCREMENTAL.

La API pública no expone un query `since=` ni un id incremental.
El modo incremental entonces:

1. Consulta el snapshot actual (mismos endpoints que FULL).
2. Lee el watermark persistido (última fecha de modificación procesada).
3. Se queda solo con filas cuya `fechaActualizacion` es posterior al watermark.

El estado se guarda en data_lake/metadata/extract_watermark.json:

- watermark: max(fechaActualizacion) ya procesada
- watermark_field: nombre del campo cursor
- last_run_at: fecha/hora de la última ejecución del extractor
- last_mode / last_records: auditoría de la corrida
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import requests

from config import (
    API_BASE_URL,
    API_ENDPOINTS,
    HTTP_MAX_RETRIES,
    HTTP_TIMEOUT_SECONDS,
    WATERMARK_FIELD,
    WATERMARK_PATH,
)

logger = logging.getLogger(__name__)

ExtractMode = Literal["full", "incremental"]


class DolarApiExtractor:
    """Cliente de extracción. No escribe el lake: solo devuelve registros crudos."""

    def __init__(
        self,
        base_url: str = API_BASE_URL,
        timeout: int = HTTP_TIMEOUT_SECONDS,
        max_retries: int = HTTP_MAX_RETRIES,
        watermark_path: Path = WATERMARK_PATH,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.watermark_path = watermark_path
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "tp-ingdatos-elt/2.0"}
        )

    def extract(self, mode: ExtractMode) -> list[dict[str, Any]]:
        """Punto único de extracción.

        Args:
            mode: ``full`` trae todo. ``incremental`` filtra por watermark.

        Returns:
            Lista de dicts tal como los devolvió la API, más origen/endpoint.
        """
        if mode == "full":
            return self.extract_full()
        if mode == "incremental":
            return self.extract_incremental()
        raise ValueError(f"Modo de extracción no soportado: {mode}")

    def extract_full(self) -> list[dict[str, Any]]:
        """Descarga el snapshot completo de todos los endpoints configurados."""
        records: list[dict[str, Any]] = []
        for source_name, path in API_ENDPOINTS:
            payload = self._get_json(path)
            items = payload if isinstance(payload, list) else [payload]
            logger.info("FULL | endpoint %s: %s registros", path, len(items))
            for item in items:
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                row["_endpoint"] = path
                row["_source_name"] = source_name
                records.append(row)
        return records

    def extract_incremental(self) -> list[dict[str, Any]]:
        """Devuelve solo cotizaciones nuevas/modificadas vs el watermark.

        Si no hay watermark (primera corrida), se comporta como FULL para
        no dejar el lake vacío.
        """
        watermark = self.load_watermark()
        snapshot = self.extract_full()
        if watermark is None:
            logger.info(
                "INCREMENTAL | sin watermark previo: se entrega el snapshot completo"
            )
            return snapshot

        filtered: list[dict[str, Any]] = []
        for row in snapshot:
            updated_at = _parse_api_timestamp(row.get(WATERMARK_FIELD))
            if updated_at is not None and updated_at > watermark:
                filtered.append(row)
        logger.info(
            "INCREMENTAL | campo=%s | watermark=%s | nuevas=%s de %s",
            WATERMARK_FIELD,
            watermark.isoformat(),
            len(filtered),
            len(snapshot),
        )
        return filtered

    def load_watermark(self) -> datetime | None:
        """Lee la última fecha de modificación ya procesada.

        Returns:
            datetime UTC del watermark, o None si nunca se persistió estado.
        """
        state = self._read_state()
        raw = state.get("watermark")
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw))
        except ValueError:
            logger.warning("Watermark inválido en %s. Se ignora.", self.watermark_path)
            return None

    def save_watermark(
        self,
        records: list[dict[str, Any]],
        mode: ExtractMode = "incremental",
    ) -> None:
        """Persiste watermark + fecha de la última ejecución.

        - Si el lote trae fechas, el watermark avanza al max(fechaActualizacion).
        - Si el lote viene vacío (incremental sin novedades), se conserva el
          watermark anterior y solo se actualiza last_run_at.
        """
        previous = self._read_state()
        timestamps = [
            ts
            for ts in (_parse_api_timestamp(row.get(WATERMARK_FIELD)) for row in records)
            if ts is not None
        ]
        latest = max(timestamps) if timestamps else None
        if latest is None:
            previous_wm = previous.get("watermark")
            latest_text = previous_wm
        else:
            latest_text = latest.isoformat()

        state = {
            "watermark": latest_text,
            "watermark_field": WATERMARK_FIELD,
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "last_mode": mode,
            "last_records": len(records),
        }
        self.watermark_path.parent.mkdir(parents=True, exist_ok=True)
        self.watermark_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(
            "Estado de extracción | watermark=%s | last_run_at=%s | mode=%s | records=%s",
            state["watermark"],
            state["last_run_at"],
            mode,
            len(records),
        )

    def _read_state(self) -> dict[str, Any]:
        if not self.watermark_path.exists():
            return {}
        try:
            payload = json.loads(self.watermark_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("No se pudo leer watermark (%s). Se ignora.", exc)
            return {}
        return payload if isinstance(payload, dict) else {}

    def _get_json(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                wait_seconds = 2 ** (attempt - 1)
                logger.warning(
                    "Fallo GET %s (intento %s/%s): %s. Reintento en %ss",
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
        raise RuntimeError(f"No se pudo extraer {url}") from last_error


def _parse_api_timestamp(value: Any) -> datetime | None:
    """Convierte fechaActualizacion de la API a datetime UTC. No muta el registro."""
    if value is None or value == "":
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

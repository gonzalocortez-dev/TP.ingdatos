"""Extracción desde DolarAPI: modo FULL e INCREMENTAL.

La API pública no expone un query `since=` ni un id incremental.
El modo incremental entonces:

1. Consulta el snapshot actual (mismos endpoints que FULL).
2. Chequea HTTP, JSON y campos obligatorios de cada registro.
3. Lee el watermark persistido (última fecha de modificación procesada).
4. Se queda solo con filas cuya `fechaActualizacion` es posterior al watermark.

El estado se guarda en data_lake/metadata/extract_watermark.json.
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

# Contrato mínimo de DolarAPI. Si falta alguno, el registro no entra al lake.
REQUIRED_API_FIELDS: tuple[str, ...] = (
    "moneda",
    "casa",
    "nombre",
    "compra",
    "venta",
    "fechaActualizacion",
)


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
            Lista de dicts validados, más origen/endpoint.
        """
        if mode == "full":
            return self.extract_full()
        if mode == "incremental":
            return self.extract_incremental()
        raise ValueError(f"Modo de extracción no soportado: {mode}")

    def extract_full(self) -> list[dict[str, Any]]:
        """Descarga y valida el snapshot completo de todos los endpoints."""
        records: list[dict[str, Any]] = []
        for source_name, path in API_ENDPOINTS:
            items = self._fetch_validated_records(path)
            logger.info("FULL | endpoint %s: %s registros válidos", path, len(items))
            for item in items:
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
            updated_at = parse_api_timestamp(row.get(WATERMARK_FIELD))
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
        """Lee la última fecha de modificación ya procesada."""
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

        Si el lote viene vacío se conserva el watermark anterior y solo
        se actualiza last_run_at.
        """
        previous = self._read_state()
        timestamps = [
            ts
            for ts in (parse_api_timestamp(row.get(WATERMARK_FIELD)) for row in records)
            if ts is not None
        ]
        latest = max(timestamps) if timestamps else None
        latest_text = latest.isoformat() if latest is not None else previous.get("watermark")

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

    def _fetch_validated_records(self, path: str) -> list[dict[str, Any]]:
        """GET + chequeo de HTTP/JSON + validación de campos de negocio."""
        payload = self._get_json(path)
        return validate_api_payload(payload, endpoint=path)

    def _get_json(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                return parse_successful_json_response(response, endpoint=path)
            except (requests.RequestException, ValueError, RuntimeError) as exc:
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


def parse_successful_json_response(
    response: requests.Response,
    endpoint: str,
) -> Any:
    """Chequea status HTTP y que el cuerpo sea JSON.

    Args:
        response: respuesta cruda de ``requests``.
        endpoint: path consultado, solo para el mensaje de error.

    Raises:
        RuntimeError: si el status no es 200 o el cuerpo no es JSON.
    """
    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} en {endpoint}: {response.text[:200]}"
        )
    content_type = response.headers.get("Content-Type", "")
    if content_type and "json" not in content_type.lower():
        logger.warning(
            "Content-Type inesperado en %s: %s. Se intenta parsear igual.",
            endpoint,
            content_type,
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"La respuesta de {endpoint} no es JSON válido") from exc
    logger.info("Chequeo HTTP OK | endpoint=%s | status=%s", endpoint, response.status_code)
    return payload


def validate_api_payload(payload: Any, endpoint: str) -> list[dict[str, Any]]:
    """Valida forma y campos obligatorios del payload de un endpoint.

    - Debe ser lista (o un objeto único, que se envuelve).
    - No puede venir vacía.
    - Cada ítem debe ser dict y traer REQUIRED_API_FIELDS.
    Los registros incompletos se descartan y se loguean; si no queda
    ninguno válido, se aborta la extracción de ese endpoint.
    """
    if payload is None:
        raise RuntimeError(f"Endpoint {endpoint} devolvió null")

    items = payload if isinstance(payload, list) else [payload]
    if not items:
        raise RuntimeError(f"Endpoint {endpoint} devolvió una lista vacía")

    valid_records: list[dict[str, Any]] = []
    discarded = 0
    for item in items:
        if not isinstance(item, dict):
            discarded += 1
            logger.warning("Registro no-objeto descartado en %s: %s", endpoint, item)
            continue
        missing_fields = [field for field in REQUIRED_API_FIELDS if field not in item]
        if missing_fields:
            discarded += 1
            logger.warning(
                "Registro inválido en %s, faltan %s",
                endpoint,
                missing_fields,
            )
            continue
        valid_records.append(item)

    if not valid_records:
        raise RuntimeError(
            f"Ningún registro válido en {endpoint} (recibidos={len(items)}, descartados={discarded})"
        )
    logger.info(
        "Chequeo payload OK | endpoint=%s | recibidos=%s | válidos=%s | descartados=%s",
        endpoint,
        len(items),
        len(valid_records),
        discarded,
    )
    return valid_records


def parse_api_timestamp(value: Any) -> datetime | None:
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

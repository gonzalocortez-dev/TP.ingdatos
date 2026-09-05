"""Construcción de la capa Bronze: datos crudos + metadatos de ingestión."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pandas as pd

from src.clients.dolar_api import RawQuote
from src.transforms.common import hash_payload


def build_bronze_frame(
    quotes: list[RawQuote],
    ingest_ts: datetime | None = None,
    ingest_id: str | None = None,
) -> pd.DataFrame:
    """Convierte la respuesta de la API en filas Bronze append-only.

    No se limpian ni tipan los campos de negocio: se guarda el JSON original
    para poder reprocesar la capa Silver si cambia la lógica.

    Args:
        quotes: Cotizaciones crudas obtenidas del cliente.
        ingest_ts: Timestamp UTC de la corrida. Si es None, se usa ahora.
        ingest_id: Identificador del lote. Si es None, se genera un UUID.

    Returns:
        DataFrame particionable por year/month/day/source.
    """
    ingest_ts = ingest_ts or datetime.now(timezone.utc)
    ingest_id = ingest_id or str(uuid.uuid4())

    rows = [
        {
            "ingest_id": ingest_id,
            "ingest_ts": ingest_ts.isoformat(),
            "source": quote.source,
            "endpoint": quote.endpoint,
            "payload_json": json.dumps(quote.payload, ensure_ascii=False),
            "record_hash": hash_payload(quote.payload),
            "year": ingest_ts.year,
            "month": ingest_ts.month,
            "day": ingest_ts.day,
        }
        for quote in quotes
    ]
    columns = [
        "ingest_id",
        "ingest_ts",
        "source",
        "endpoint",
        "payload_json",
        "record_hash",
        "year",
        "month",
        "day",
    ]
    return pd.DataFrame(rows, columns=columns)

"""Fixtures compartidas para las pruebas."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from src.clients.dolar_api import RawQuote
from src.config import Settings
from src.transforms.bronze import build_bronze_frame


def make_settings(tmp_path) -> Settings:
    return Settings(
        lake_root=tmp_path / "lake",
        api_base_url="https://dolarapi.com",
        http_timeout_seconds=5,
        http_max_retries=1,
        stream_interval_seconds=1,
        log_level="INFO",
    )


def sample_payloads() -> list[dict]:
    return [
        {
            "moneda": "USD",
            "casa": "oficial",
            "nombre": "Oficial",
            "compra": 1480,
            "venta": 1530,
            "fechaActualizacion": "2026-09-04T18:55:00.000Z",
        },
        {
            "moneda": "USD",
            "casa": "blue",
            "nombre": "Blue",
            "compra": 1520,
            "venta": 1540,
            "fechaActualizacion": "2026-09-05T20:56:00.000Z",
        },
        {
            "moneda": "EUR",
            "casa": "oficial",
            "nombre": "Euro",
            "compra": 1739.13,
            "venta": 1753.35,
            "fechaActualizacion": "2026-09-04T16:57:00.000Z",
        },
    ]


@pytest.fixture
def raw_quotes() -> list[RawQuote]:
    dolares = sample_payloads()[:2]
    cotizaciones = [sample_payloads()[0], sample_payloads()[2]]
    quotes = [RawQuote("dolares", "/v1/dolares", item) for item in dolares]
    quotes.extend(RawQuote("cotizaciones", "/v1/cotizaciones", item) for item in cotizaciones)
    return quotes


@pytest.fixture
def bronze_df(raw_quotes) -> pd.DataFrame:
    return build_bronze_frame(
        raw_quotes,
        ingest_ts=datetime(2026, 9, 5, 21, 0, tzinfo=timezone.utc),
        ingest_id="test-batch-1",
    )


@pytest.fixture
def duplicated_bronze(bronze_df) -> pd.DataFrame:
    clone = bronze_df.copy()
    clone["ingest_id"] = "test-batch-2"
    return pd.concat([bronze_df, clone], ignore_index=True)

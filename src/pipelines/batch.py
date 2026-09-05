"""Pipeline batch: API → Bronze → Silver → Gold."""

from __future__ import annotations

import logging

from src.clients.dolar_api import DolarApiClient
from src.config import Settings, get_settings
from src.storage.parquet_store import ParquetLake
from src.transforms.bronze import build_bronze_frame
from src.transforms.gold import build_gold_tables
from src.transforms.silver import bronze_to_silver

logger = logging.getLogger(__name__)


def run_batch(settings: Settings | None = None) -> dict[str, int]:
    """Ejecuta una corrida completa de las tres capas medallion.

    Returns:
        Conteo de filas por capa / tabla Gold.
    """
    settings = settings or get_settings()
    client = DolarApiClient(settings)
    lake = ParquetLake(settings)

    logger.info("Ingestión batch desde %s", settings.api_base_url)
    raw_quotes = client.fetch_all()
    bronze_df = build_bronze_frame(raw_quotes)
    lake.append_bronze(bronze_df)

    silver_df = bronze_to_silver(bronze_df)
    lake.merge_silver(silver_df)

    silver_all = lake.read_silver()
    gold_tables = build_gold_tables(silver_all)
    lake.write_gold(gold_tables)

    status = lake.status()
    logger.info("Batch finalizado | %s", status)
    return status

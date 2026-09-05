"""Pruebas de particionado y merge del lake Parquet."""

from __future__ import annotations

from src.storage.parquet_store import ParquetLake
from src.transforms.gold import build_gold_tables
from src.transforms.silver import bronze_to_silver
from tests.conftest import make_settings


def test_lake_partitions_and_prevents_duplicates(tmp_path, bronze_df, duplicated_bronze) -> None:
    lake = ParquetLake(make_settings(tmp_path))

    lake.append_bronze(bronze_df)
    second_batch = bronze_df.copy()
    second_batch["ingest_id"] = "test-batch-2"
    lake.append_bronze(second_batch)
    bronze = lake.read_bronze()
    assert len(bronze) == len(bronze_df) * 2

    silver = bronze_to_silver(duplicated_bronze)
    lake.merge_silver(silver)
    lake.merge_silver(silver)
    stored = lake.read_silver()
    assert stored["business_key"].is_unique

    partition_dirs = list((tmp_path / "lake" / "silver" / "quotes").rglob("casa=*"))
    assert partition_dirs

    gold = build_gold_tables(stored)
    lake.write_gold(gold)
    status = lake.status()
    assert status["bronze"] > 0
    assert status["silver"] == len(stored)
    assert status["gold_latest"] == len(gold["latest"])

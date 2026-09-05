"""Lakehouse local en Parquet con particionado Hive.

Bronze es append-only (auditoría). Silver se fusiona por clave de negocio
para no repetir hechos. Gold se regenera completo: el volumen es chico y
así se garantiza consistencia analítica.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from src.config import Settings, get_settings
from src.transforms.silver import SILVER_EMPTY_COLUMNS, deduplicate_quotes

logger = logging.getLogger(__name__)

BRONZE_PARTITION_COLS = ["year", "month", "day", "source"]
SILVER_PARTITION_COLS = ["year", "month", "casa"]


class ParquetLake:
    """Abstracción de lectura/escritura del data lake local."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.settings.lake_root.mkdir(parents=True, exist_ok=True)

    def append_bronze(self, bronze_df: pd.DataFrame) -> Path:
        """Persiste un lote Bronze sin pisar lotes anteriores."""
        if bronze_df.empty:
            logger.info("Bronze: no hay filas para escribir")
            return self.settings.bronze_path

        self._write_dataset(
            bronze_df,
            self.settings.bronze_path,
            BRONZE_PARTITION_COLS,
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"{uuid.uuid4().hex}-{{i}}.parquet",
        )
        logger.info("Bronze: se escribieron %s filas en %s", len(bronze_df), self.settings.bronze_path)
        return self.settings.bronze_path

    def merge_silver(self, silver_df: pd.DataFrame) -> int:
        """Upsert Silver: combina con lo existente y deduplica.

        Returns:
            Cantidad de filas Silver resultantes.
        """
        if silver_df.empty:
            logger.info("Silver: lote vacío, no se fusiona")
            existing = self.read_silver()
            return len(existing)

        existing = self.read_silver()
        combined = (
            silver_df
            if existing.empty
            else pd.concat([existing, silver_df], ignore_index=True)
        )
        combined = deduplicate_quotes(combined)
        self._replace_dataset(combined, self.settings.silver_path, SILVER_PARTITION_COLS)
        logger.info("Silver: %s filas vigentes particionadas por year/month/casa", len(combined))
        return len(combined)

    def write_gold(self, tables: dict[str, pd.DataFrame]) -> Path:
        """Reescribe las tablas Gold (una carpeta por modelo)."""
        gold_root = self.settings.gold_path
        gold_root.mkdir(parents=True, exist_ok=True)

        for name, frame in tables.items():
            target = gold_root / name
            if target.exists():
                self._remove_tree(target)
            if frame.empty:
                logger.info("Gold/%s: sin datos", name)
                continue
            target.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(target / "data.parquet", index=False)
            logger.info("Gold/%s: %s filas", name, len(frame))
        return gold_root

    def read_bronze(self) -> pd.DataFrame:
        return self._read_dataset(self.settings.bronze_path)

    def read_silver(self) -> pd.DataFrame:
        frame = self._read_dataset(self.settings.silver_path)
        if frame.empty:
            return pd.DataFrame(columns=SILVER_EMPTY_COLUMNS)
        return frame

    def read_gold(self, name: str) -> pd.DataFrame:
        path = self.settings.gold_path / name / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def status(self) -> dict[str, int]:
        """Resumen de filas por capa para diagnóstico rápido."""
        gold_counts = {}
        gold_root = self.settings.gold_path
        if gold_root.exists():
            for child in gold_root.iterdir():
                parquet_file = child / "data.parquet"
                if parquet_file.exists():
                    gold_counts[child.name] = len(pd.read_parquet(parquet_file))
        return {
            "bronze": len(self.read_bronze()),
            "silver": len(self.read_silver()),
            **{f"gold_{name}": count for name, count in gold_counts.items()},
        }

    def _write_dataset(
        self,
        frame: pd.DataFrame,
        path: Path,
        partition_cols: list[str],
        existing_data_behavior: str,
        basename_template: str = "part-{i}.parquet",
    ) -> None:
        path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(frame, preserve_index=False)
        partitioning = ds.partitioning(
            pa.schema([(col, table.schema.field(col).type) for col in partition_cols]),
            flavor="hive",
        )
        ds.write_dataset(
            table,
            path,
            format="parquet",
            partitioning=partitioning,
            existing_data_behavior=existing_data_behavior,
            basename_template=basename_template,
        )

    def _replace_dataset(self, frame: pd.DataFrame, path: Path, partition_cols: list[str]) -> None:
        if path.exists():
            self._remove_tree(path)
        if frame.empty:
            return
        self._write_dataset(frame, path, partition_cols, existing_data_behavior="overwrite_or_ignore")

    @staticmethod
    def _read_dataset(path: Path) -> pd.DataFrame:
        if not path.exists() or not any(path.rglob("*.parquet")):
            return pd.DataFrame()
        dataset = ds.dataset(path, format="parquet", partitioning="hive")
        return dataset.to_table().to_pandas()

    @staticmethod
    def _remove_tree(path: Path) -> None:
        if not path.exists():
            return
        if path.is_file():
            path.unlink()
            return
        for child in path.rglob("*"):
            if child.is_file():
                child.unlink()
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_dir():
                child.rmdir()
        if path.exists():
            path.rmdir()

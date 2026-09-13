"""Capa Bronze: Load del ELT.

Propósito de esta capa
----------------------
Guardar los datos **crudos** tal como llegaron de la API (schema-on-read).
Acá NO se castean precios, NO se imputan nulos, NO se calculan spreads
ni se estandarizan nombres. Eso ocurre recién al pasar a Silver.

Única excepción (metadatos de ingesta, no de negocio):
- fecha_ingesta: cuándo se cargó el lote
- origen_datos: de qué endpoint/entidad vino
- modo_extraccion / batch_id: auditoría de la corrida

Formato: Delta Lake en data_lake/bronze/quotes/

Modos de escritura
------------------
- Extracción FULL:        overwrite (reemplaza el snapshot crudo)
- Extracción INCREMENTAL: MERGE/upsert si la tabla ya existe;
                          overwrite si es la primera carga
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from config import BRONZE_QUOTES_PATH

logger = logging.getLogger(__name__)

WriteStrategy = Literal["overwrite", "merge"]

# Schema Bronze: campos de la API como STRING + columnas de control.
# fechaActualizacion se deja en camelCase a propósito (crudo).
BRONZE_SCHEMA = StructType(
    [
        StructField("moneda", StringType(), True),
        StructField("casa", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("compra", StringType(), True),
        StructField("venta", StringType(), True),
        StructField("fechaActualizacion", StringType(), True),
        StructField("raw_json", StringType(), True),
        StructField("fecha_ingesta", StringType(), True),
        StructField("origen_datos", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("modo_extraccion", StringType(), True),
        StructField("batch_id", StringType(), True),
    ]
)

# Clave natural para el MERGE incremental (misma cotización / misma fuente).
BRONZE_MERGE_KEYS = ("origen_datos", "casa", "moneda", "fechaActualizacion")


def resolve_bronze_write_strategy(ingest_mode: str, table_exists: bool) -> WriteStrategy:
    """Decide overwrite vs MERGE según el modo de extracción.

    Args:
        ingest_mode: ``full`` o ``incremental``.
        table_exists: True si ya hay una tabla Delta en Bronze.
    """
    if ingest_mode == "full":
        return "overwrite"
    if ingest_mode == "incremental":
        return "merge" if table_exists else "overwrite"
    raise ValueError(f"Modo de extracción no soportado para escritura: {ingest_mode}")


def load_bronze(
    spark: SparkSession,
    raw_records: list[dict[str, Any]],
    ingest_mode: str,
) -> int:
    """Escribe el lote crudo en Delta y devuelve la cantidad de filas.

    compra/venta/fecha se guardan como STRING a propósito: en Bronze no
    hay casteo. Silver es quien interpreta los tipos.
    """
    if not raw_records:
        logger.info("Bronze: no hay registros para cargar (lote vacío).")
        return 0

    fecha_ingesta = datetime.now(timezone.utc).isoformat()
    batch_id = str(uuid.uuid4())
    rows = [
        _to_bronze_row(record, fecha_ingesta, ingest_mode, batch_id) for record in raw_records
    ]

    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    strategy = _write_bronze(spark, df, ingest_mode)
    logger.info(
        "Bronze LOAD | filas=%s | modo=%s | write=%s | batch_id=%s | path=%s",
        len(rows),
        ingest_mode,
        strategy,
        batch_id,
        BRONZE_QUOTES_PATH,
    )
    return len(rows)


def read_bronze(spark: SparkSession) -> DataFrame:
    """Lee la tabla Delta Bronze completa (todo el histórico crudo)."""
    path = str(BRONZE_QUOTES_PATH)
    try:
        return spark.read.format("delta").load(path)
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo leer Bronze en {path}. ¿Corriste antes la extracción/carga?"
        ) from exc


def _to_bronze_row(
    record: dict[str, Any],
    fecha_ingesta: str,
    ingest_mode: str,
    batch_id: str,
) -> dict[str, str | None]:
    """Copia los campos de la API a strings y agrega metadatos de ingesta."""
    original = {
        key: value
        for key, value in record.items()
        if not str(key).startswith("_")
    }
    origen = record.get("_source_name") or record.get("origen_datos")
    endpoint = record.get("_endpoint") or record.get("endpoint")
    return {
        "moneda": _as_text(record.get("moneda")),
        "casa": _as_text(record.get("casa")),
        "nombre": _as_text(record.get("nombre")),
        "compra": _as_text(record.get("compra")),
        "venta": _as_text(record.get("venta")),
        "fechaActualizacion": _as_text(record.get("fechaActualizacion")),
        "raw_json": json.dumps(original, ensure_ascii=False),
        "fecha_ingesta": fecha_ingesta,
        "origen_datos": _as_text(origen),
        "endpoint": _as_text(endpoint),
        "modo_extraccion": ingest_mode,
        "batch_id": batch_id,
    }


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _write_bronze(spark: SparkSession, df: DataFrame, ingest_mode: str) -> WriteStrategy:
    BRONZE_QUOTES_PATH.mkdir(parents=True, exist_ok=True)
    path = str(BRONZE_QUOTES_PATH)
    table_exists = _is_delta_table(spark, path)
    strategy = resolve_bronze_write_strategy(ingest_mode, table_exists)

    if strategy == "overwrite":
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        return strategy

    _merge_bronze(spark, df, path)
    return strategy


def _merge_bronze(spark: SparkSession, incoming: DataFrame, path: str) -> None:
    """Upsert incremental: actualiza la cotización si ya existe, si no la inserta."""
    from delta.tables import DeltaTable

    merge_condition = " AND ".join(
        f"target.{key} <=> source.{key}" for key in BRONZE_MERGE_KEYS
    )
    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(incoming.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    try:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return (BRONZE_QUOTES_PATH / "_delta_log").exists()

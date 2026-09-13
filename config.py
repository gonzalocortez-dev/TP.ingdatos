"""Configuración global del pipeline ELT (DolarAPI → Delta Lake).

Este módulo no transforma datos: centraliza URLs, rutas de capas
y la configuración inicial de PySpark + Delta Lake.

Importante: las keys de Spark/Delta viven acá (no hardcodeadas en
spark_session.py) para evitar errores de compatibilidad si se
desalinean versiones o falta el catálogo Delta.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Fuente (Extract)
# ---------------------------------------------------------------------------
API_BASE_URL = os.getenv("DOLAR_API_BASE_URL", "https://dolarapi.com").rstrip("/")
API_ENDPOINTS: tuple[tuple[str, str], ...] = (
    ("dolares", "/v1/dolares"),
    ("cotizaciones", "/v1/cotizaciones"),
)
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))

# Campo de la API que usamos como cursor incremental (fecha de modificación).
WATERMARK_FIELD = "fechaActualizacion"

# ---------------------------------------------------------------------------
# Data Lake (Load / Transform)
# Rutas alineadas a la consigna: data_lake/<capa>/<entidad>/
# Cada carpeta es una tabla Delta (no CSV, no Parquet suelto).
# ---------------------------------------------------------------------------
LAKE_ROOT = Path(os.getenv("LAKE_ROOT", "data_lake"))
BRONZE_QUOTES_PATH = LAKE_ROOT / "bronze" / "quotes"
SILVER_QUOTES_PATH = LAKE_ROOT / "silver" / "quotes"
GOLD_LATEST_PATH = LAKE_ROOT / "gold" / "latest"
GOLD_DAILY_PATH = LAKE_ROOT / "gold" / "daily_metrics"
GOLD_BRECHA_PATH = LAKE_ROOT / "gold" / "brecha"
WATERMARK_PATH = LAKE_ROOT / "metadata" / "extract_watermark.json"

# ---------------------------------------------------------------------------
# PySpark / Delta Lake — configuración inicial de la sesión
# ---------------------------------------------------------------------------
# Compatibilidad verificada (ver requirements.txt):
#   pyspark==3.5.3  +  delta-spark==3.2.1
# Delta 3.2.x requiere Spark 3.5.x. Si se mezclan versiones, falla el
# classloader o el write.format("delta").
#
# Estas keys se aplican ANTES de SparkSession.getOrCreate().
# Sin spark.sql.extensions + spark_catalog, Spark trata el path como
# Parquet común y no crea _delta_log.
# ---------------------------------------------------------------------------
SPARK_APP_NAME = "tp-ingdatos-elt-medallion"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_TIMEZONE = "UTC"
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
DELTA_LOG_CHECKPOINT_INTERVAL = "10"

SPARK_DELTA_CONFIG: dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.checkpointInterval": DELTA_LOG_CHECKPOINT_INTERVAL,
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.session.timeZone": SPARK_TIMEZONE,
    "spark.sql.shuffle.partitions": SPARK_SHUFFLE_PARTITIONS,
}


def ensure_lake_dirs() -> None:
    """Crea el árbol de carpetas del lake si todavía no existe."""
    for path in (
        BRONZE_QUOTES_PATH,
        SILVER_QUOTES_PATH,
        GOLD_LATEST_PATH,
        GOLD_DAILY_PATH,
        GOLD_BRECHA_PATH,
        WATERMARK_PATH.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)

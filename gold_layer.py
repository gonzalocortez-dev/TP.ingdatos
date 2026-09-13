"""Persistencia Silver y Gold en Delta Lake.

Silver
------
Datos limpios, tipados, deduplicados. Es la tabla de hechos de negocio.
Se escribe en data_lake/silver/quotes/. Siempre overwrite: se regenera
desde todo Bronze (ya sea snapshot full o histórico + MERGE incremental).

Gold
----
Modelos analíticos / OLAP a partir de Silver válido:
- latest: última cotización por casa y moneda
- daily_metrics: min/max/promedio diario
- brecha: diferencia vs el dólar oficial

Se escriben en data_lake/gold/<entidad>/ también como Delta.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import (
    GOLD_BRECHA_PATH,
    GOLD_DAILY_PATH,
    GOLD_LATEST_PATH,
    SILVER_QUOTES_PATH,
)
from transform_layer import select_silver_columns

logger = logging.getLogger(__name__)


def persist_silver(silver_df: DataFrame) -> None:
    """Guarda Silver en Delta. Overwrite: se regenera desde Bronze limpio."""
    SILVER_QUOTES_PATH.mkdir(parents=True, exist_ok=True)
    out = select_silver_columns(silver_df)
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month", "casa")
        .save(str(SILVER_QUOTES_PATH))
    )
    logger.info("Silver | escrito Delta en %s", SILVER_QUOTES_PATH)


def read_silver(spark: SparkSession) -> DataFrame:
    """Lee la tabla Delta Silver."""
    return spark.read.format("delta").load(str(SILVER_QUOTES_PATH))


def build_and_persist_gold(silver_df: DataFrame) -> dict[str, int]:
    """Construye las tablas Gold y las persiste en Delta.

    Returns:
        Cantidad de filas por entidad Gold.
    """
    valid = silver_df.filter(F.col("quality_ok") == True)  # noqa: E712
    latest = _build_latest(valid)
    daily = _build_daily_metrics(valid)
    brecha = _build_brecha(latest)

    _write_gold(latest, GOLD_LATEST_PATH)
    _write_gold(daily, GOLD_DAILY_PATH)
    _write_gold(brecha, GOLD_BRECHA_PATH)

    counts = {
        "gold_latest": latest.count(),
        "gold_daily_metrics": daily.count(),
        "gold_brecha": brecha.count(),
    }
    logger.info("Gold | %s", counts)
    return counts


def _build_latest(df: DataFrame) -> DataFrame:
    """Última cotización por casa/moneda. USD sale de /v1/dolares."""
    usd = df.filter((F.col("moneda") == "USD") & (F.col("source") == "dolares"))
    others = df.filter(F.col("moneda") != "USD")
    combined = usd.unionByName(others, allowMissingColumns=True)
    window = Window.partitionBy("casa", "moneda").orderBy(
        F.col("fecha_actualizacion").desc(), F.col("ingest_ts").desc()
    )
    return (
        combined.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _build_daily_metrics(df: DataFrame) -> DataFrame:
    """Agregados diarios para consumo tipo OLAP."""
    return (
        df.groupBy("year", "month", "day", "casa", "moneda", "source")
        .agg(
            F.count("*").alias("cotizaciones"),
            F.avg("compra").alias("compra_promedio"),
            F.min("venta").alias("venta_min"),
            F.max("venta").alias("venta_max"),
            F.avg("venta").alias("venta_promedio"),
            F.avg("spread").alias("spread_promedio"),
        )
        .withColumn("rango_venta", F.col("venta_max") - F.col("venta_min"))
    )


def _build_brecha(latest: DataFrame) -> DataFrame:
    """Brecha de venta vs el oficial USD más reciente del snapshot."""
    oficial = (
        latest.filter((F.col("casa") == "oficial") & (F.col("moneda") == "USD"))
        .select(F.col("venta").alias("venta_oficial"))
        .limit(1)
    )
    joined = latest.crossJoin(oficial)
    return (
        joined.withColumn(
            "brecha_oficial_pct",
            F.when(
                (F.col("moneda") == "USD") & (F.col("venta_oficial") > 0),
                ((F.col("venta") - F.col("venta_oficial")) / F.col("venta_oficial")) * 100.0,
            ),
        )
        .select(
            "casa",
            "nombre",
            "moneda",
            "venta",
            "venta_oficial",
            "brecha_oficial_pct",
            "spread",
            "spread_pct",
            "fecha_actualizacion",
        )
    )


def _write_gold(df: DataFrame, path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )
    logger.info("Gold | escrito Delta en %s", path)

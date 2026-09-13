"""Persistencia Silver y Gold en Delta Lake.

Silver
------
Hechos de negocio limpios. Overwrite desde todo Bronze.
Particionado por year/month/casa: recortes por fecha y mercado
sin leer la tabla entera.

Gold
----
Modelos analíticos / OLAP a partir de Silver válido:
- latest: última cotización por casa y moneda (tabla chica, sin partición)
- daily_metrics: agregados diarios (particionado year/month)
- brecha: diferencia vs el dólar oficial (tabla chica, sin partición)
"""

from __future__ import annotations

import logging
from pathlib import Path

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
    """Guarda Silver en Delta, particionado para lecturas por fecha/casa."""
    SILVER_QUOTES_PATH.mkdir(parents=True, exist_ok=True)
    silver_contract = select_silver_columns(silver_df)
    (
        silver_contract.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month", "casa")
        .save(str(SILVER_QUOTES_PATH))
    )
    logger.info("Silver | escrito Delta particionado year/month/casa en %s", SILVER_QUOTES_PATH)


def read_silver(spark: SparkSession) -> DataFrame:
    """Lee la tabla Delta Silver."""
    return spark.read.format("delta").load(str(SILVER_QUOTES_PATH))


def build_and_persist_gold(silver_df: DataFrame) -> dict[str, int]:
    """Construye las tablas Gold y las persiste en Delta."""
    valid_quotes = silver_df.filter(F.col("quality_ok") == True)  # noqa: E712
    latest_quotes = build_latest_quotes(valid_quotes)
    daily_metrics = build_daily_metrics(valid_quotes)
    official_gap = build_official_gap(latest_quotes)

    write_gold_table(latest_quotes, GOLD_LATEST_PATH)
    write_gold_table(daily_metrics, GOLD_DAILY_PATH, partition_columns=("year", "month"))
    write_gold_table(official_gap, GOLD_BRECHA_PATH)

    counts = {
        "gold_latest": latest_quotes.count(),
        "gold_daily_metrics": daily_metrics.count(),
        "gold_brecha": official_gap.count(),
    }
    logger.info("Gold | %s", counts)
    return counts


def build_latest_quotes(valid_quotes: DataFrame) -> DataFrame:
    """Última cotización por casa/moneda. USD sale de /v1/dolares."""
    usd_from_dolares = valid_quotes.filter(
        (F.col("moneda") == "USD") & (F.col("origen_datos") == "dolares")
    )
    non_usd_quotes = valid_quotes.filter(F.col("moneda") != "USD")
    combined_quotes = usd_from_dolares.unionByName(non_usd_quotes, allowMissingColumns=True)
    latest_per_market = Window.partitionBy("casa", "moneda").orderBy(
        F.col("fecha_actualizacion").desc(), F.col("fecha_ingesta").desc()
    )
    return (
        combined_quotes.withColumn("row_version", F.row_number().over(latest_per_market))
        .filter(F.col("row_version") == 1)
        .drop("row_version")
    )


def build_daily_metrics(valid_quotes: DataFrame) -> DataFrame:
    """Agregados diarios para consumo tipo OLAP."""
    return (
        valid_quotes.groupBy("year", "month", "day", "casa", "moneda", "origen_datos")
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


def build_official_gap(latest_quotes: DataFrame) -> DataFrame:
    """Brecha de venta vs el oficial USD más reciente del snapshot."""
    official_usd = (
        latest_quotes.filter((F.col("casa") == "oficial") & (F.col("moneda") == "USD"))
        .select(F.col("venta").alias("venta_oficial"))
        .limit(1)
    )
    quotes_with_official = latest_quotes.crossJoin(official_usd)
    return (
        quotes_with_official.withColumn(
            "brecha_oficial_pct",
            F.when(
                (F.col("moneda") == "USD") & (F.col("venta_oficial") > 0),
                ((F.col("venta") - F.col("venta_oficial")) / F.col("venta_oficial")) * 100.0,
            ),
        ).select(
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


def write_gold_table(
    gold_df: DataFrame,
    destination: Path,
    partition_columns: tuple[str, ...] | None = None,
) -> None:
    """Persiste una entidad Gold. Solo particiona si el volumen/consulta lo justifica."""
    destination.mkdir(parents=True, exist_ok=True)
    writer = (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(str(destination))
    logger.info(
        "Gold | escrito Delta en %s | particiones=%s",
        destination,
        partition_columns or "ninguna",
    )

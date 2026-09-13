"""Transformaciones PySpark: de Bronze (crudo) a Silver (limpio).

Se ejecutan DESPUÉS del Load. Es el paso T del ELT.

Tareas de transformación (más de cuatro)
----------------------------------------
1. Casteo y normalización de tipos / texto.
2. Estandarización de fechas a UTC.
3. Tratamiento de nulos categóricos.
4. Columnas de negocio (spread, mid_price, tags, particiones).
5. Reglas de calidad (quality_ok) sin borrar filas.
6. Deduplicación por clave de negocio.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Aplica la cadena de limpieza sobre el histórico Bronze."""
    cleaned = cast_and_normalize_types(bronze_df)
    cleaned = standardize_timestamps_to_utc(cleaned)
    cleaned = fill_categorical_nulls(cleaned)
    cleaned = add_business_columns(cleaned)
    cleaned = flag_data_quality(cleaned)
    cleaned = deduplicate_by_business_key(cleaned)
    logger.info("Transform | Bronze → Silver listo")
    return cleaned


def cast_and_normalize_types(bronze_df: DataFrame) -> DataFrame:
    """Castea precios y normaliza texto. Todavía no interpreta zonas horarias."""
    return (
        bronze_df.select(
            F.col("origen_datos"),
            F.col("endpoint"),
            F.col("fecha_ingesta").alias("fecha_ingesta_raw"),
            F.col("modo_extraccion"),
            F.col("batch_id"),
            F.trim(F.col("moneda")).alias("moneda_raw"),
            F.trim(F.col("casa")).alias("casa_raw"),
            F.trim(F.col("nombre")).alias("nombre_raw"),
            F.col("compra").alias("compra_raw"),
            F.col("venta").alias("venta_raw"),
            F.col("fechaActualizacion").alias("fecha_raw"),
            F.col("raw_json"),
        )
        .withColumn("moneda", F.upper(F.col("moneda_raw")))
        .withColumn("casa", F.lower(F.col("casa_raw")))
        .withColumn("nombre", F.initcap(F.col("nombre_raw")))
        .withColumn("compra", F.col("compra_raw").cast("double"))
        .withColumn("venta", F.col("venta_raw").cast("double"))
        .withColumn("fecha_actualizacion_naive", F.to_timestamp(F.col("fecha_raw")))
        .withColumn("fecha_ingesta_naive", F.to_timestamp(F.col("fecha_ingesta_raw")))
    )


def standardize_timestamps_to_utc(quotes_df: DataFrame) -> DataFrame:
    """Lleva fechas de cotización e ingesta a UTC (sesión Spark ya es UTC)."""
    return (
        quotes_df.withColumn(
            "fecha_actualizacion",
            F.to_utc_timestamp(F.col("fecha_actualizacion_naive"), "UTC"),
        ).withColumn(
            "fecha_ingesta",
            F.to_utc_timestamp(F.col("fecha_ingesta_naive"), "UTC"),
        )
    )


def fill_categorical_nulls(quotes_df: DataFrame) -> DataFrame:
    """Completa categóricos. Los precios nulos se dejan nulos y se marcan después."""
    return (
        quotes_df.fillna({"casa": "desconocida", "nombre": "N/A", "moneda": "N/A"})
        .withColumn(
            "fecha_actualizacion",
            F.coalesce(
                F.col("fecha_actualizacion"),
                F.to_utc_timestamp(F.to_timestamp(F.lit("1900-01-01 00:00:00")), "UTC"),
            ),
        )
    )


def add_business_columns(quotes_df: DataFrame) -> DataFrame:
    """Métricas, tags de mercado y columnas de partición."""
    market_tags = (
        F.when(F.col("casa") == "oficial", F.lit("regulado,banco,bcra"))
        .when(F.col("casa") == "blue", F.lit("paralelo,informal"))
        .when(F.col("casa") == "bolsa", F.lit("financiero,merval,mep"))
        .when(F.col("casa") == "contadoconliqui", F.lit("financiero,ccl,bursatil"))
        .when(F.col("casa") == "mayorista", F.lit("regulado,interbancario"))
        .when(F.col("casa") == "cripto", F.lit("digital,crypto"))
        .when(F.col("casa") == "tarjeta", F.lit("regulado,impuesto,turismo"))
        .otherwise(F.lit("otro"))
    )
    return (
        quotes_df.withColumn("spread", F.col("venta") - F.col("compra"))
        .withColumn("mid_price", (F.col("venta") + F.col("compra")) / F.lit(2.0))
        .withColumn(
            "spread_pct",
            F.when(F.col("compra") > 0, (F.col("spread") / F.col("compra")) * 100.0),
        )
        .withColumn("market_tags", market_tags)
        .withColumn("year", F.year("fecha_actualizacion"))
        .withColumn("month", F.month("fecha_actualizacion"))
        .withColumn("day", F.dayofmonth("fecha_actualizacion"))
        .withColumn(
            "business_key",
            F.concat_ws(
                "|",
                F.col("origen_datos"),
                F.col("casa"),
                F.col("moneda"),
                F.col("fecha_actualizacion").cast("string"),
            ),
        )
    )


def flag_data_quality(quotes_df: DataFrame) -> DataFrame:
    """Marca calidad sin borrar filas: Silver conserva trazabilidad."""
    sentinel_date = F.to_utc_timestamp(F.to_timestamp(F.lit("1900-01-01 00:00:00")), "UTC")
    invalid = (
        F.col("compra").isNull()
        | F.col("venta").isNull()
        | (F.col("compra") < 0)
        | (F.col("venta") < 0)
        | (F.col("venta") < F.col("compra"))
        | (F.col("fecha_actualizacion") == sentinel_date)
    )
    return quotes_df.withColumn("quality_ok", ~invalid)


def deduplicate_by_business_key(quotes_df: DataFrame) -> DataFrame:
    """Una fila vigente por business_key: gana la ingestión más reciente."""
    latest_ingest_window = Window.partitionBy("business_key").orderBy(
        F.col("fecha_ingesta").desc_nulls_last()
    )
    return (
        quotes_df.withColumn("row_version", F.row_number().over(latest_ingest_window))
        .filter(F.col("row_version") == 1)
        .drop("row_version")
    )


def select_silver_columns(quotes_df: DataFrame) -> DataFrame:
    """Contrato de columnas de la tabla Silver."""
    return quotes_df.select(
        "business_key",
        "origen_datos",
        "endpoint",
        "modo_extraccion",
        "batch_id",
        "moneda",
        "casa",
        "nombre",
        "compra",
        "venta",
        "spread",
        "mid_price",
        "spread_pct",
        "market_tags",
        "fecha_actualizacion",
        "fecha_ingesta",
        "quality_ok",
        "year",
        "month",
        "day",
        "raw_json",
    )

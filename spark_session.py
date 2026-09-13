"""Fábrica de SparkSession con soporte Delta Lake.

La configuración vive en config.SPARK_DELTA_CONFIG para no desalinearla
del par de versiones pyspark / delta-spark.
Este módulo solo aplica esas keys y crea la sesión.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import SPARK_APP_NAME, SPARK_DELTA_CONFIG, SPARK_MASTER


def get_spark() -> SparkSession:
    """Crea (o reutiliza) una SparkSession configurada para leer/escribir Delta.

    Raises:
        RuntimeError: si falta delta-spark, no hay Java o fallan las extensiones.
    """
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:
        raise RuntimeError(
            "Falta delta-spark. Instalá las dependencias con: pip install -r requirements.txt"
        ) from exc

    builder = SparkSession.builder.appName(SPARK_APP_NAME).master(SPARK_MASTER)
    for key, value in SPARK_DELTA_CONFIG.items():
        builder = builder.config(key, value)

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:
        message = str(exc).lower()
        if "java" in message or "jdk" in message or "jvm" in message:
            raise RuntimeError(
                "PySpark necesita un JDK 11 o 17 en el PATH (JAVA_HOME). "
                "Instalá Temurin/OpenJDK, reiniciá la terminal y volvé a correr "
                "`python main.py --mode full`."
            ) from exc
        raise
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Cotizaciones AR — ELT medallion (PySpark + Delta Lake)

Pipeline de extremo a extremo sobre [DolarAPI](https://dolarapi.com/docs/) con patrón **ELT** y capas **Bronze / Silver / Gold** persistidas en **Delta Lake** (`delta-spark`).

```text
EXTRACT (API full | incremental + watermark)
    → LOAD Bronze   data_lake/bronze/quotes/     (Delta: full=overwrite, incremental=MERGE)
    → TRANSFORM     PySpark (limpieza / tipos / dedup)
    → Silver        data_lake/silver/quotes/     (Delta, hechos limpios)
    → Gold          data_lake/gold/<entidad>/    (Delta, métricas OLAP)
```

## Qué resuelve cada capa

| Capa | Propósito | Qué se guarda | Formato / escritura |
| --- | --- | --- | --- |
| **Bronze** | Raw lake / schema-on-read | Payload de la API **sin casteo ni métricas**. Precios y fechas quedan como `string`. Se agregan solo metadatos de ingesta (`fecha_ingesta`, `origen_datos`) | **Delta**: FULL → `overwrite` · INCREMENTAL → `MERGE` |
| **Silver** | Hechos de negocio | Tipos, nulos, dedup, `spread` / `mid_price`, `quality_ok` | **Delta**, overwrite particionado `year/month/casa` |
| **Gold** | Consumo analítico / OLAP | `latest`, `daily_metrics`, `brecha` | **Delta**: `daily_metrics` particionado `year/month`; `latest`/`brecha` sin partición (tablas chicas) |

Cómo se comprueba que **sí es Delta** (no Parquet suelto):

```text
data_lake/bronze/quotes/_delta_log/
data_lake/silver/quotes/_delta_log/
data_lake/gold/latest/_delta_log/
```

## Metadatos de ingesta (Bronze)

Los campos de negocio se copian tal cual. Lo único que se agrega es trazabilidad:

| Columna | Para qué |
| --- | --- |
| `fecha_ingesta` | Cuándo se cargó el lote (UTC) |
| `origen_datos` | Entidad de origen (`dolares` / `cotizaciones`) |
| `endpoint` | Path de la API |
| `modo_extraccion` | `full` o `incremental` |
| `batch_id` | Id del lote |
| `raw_json` | Payload original serializado |

## Extracción: ambos endpoints + chequeo

Se consultan `/v1/dolares` y `/v1/cotizaciones` con timeout, reintentos y `Accept: application/json`. Cada respuesta se valida:

1. HTTP 200
2. Cuerpo JSON
3. Lista no vacía
4. Campos obligatorios: `moneda`, `casa`, `nombre`, `compra`, `venta`, `fechaActualizacion`

Los registros incompletos se descartan. Si un endpoint no deja ninguno válido, la extracción aborta.

## Transformaciones Silver (`transform_layer.py`)

1. Casteo y normalización de tipos / texto
2. Fechas a UTC
3. Nulos categóricos
4. Columnas de negocio (`spread`, `mid_price`, tags, `year/month/day`)
5. Calidad (`quality_ok`) sin borrar filas
6. Deduplicación por `business_key`

## Particionado

- **Silver**: `year/month/casa` — lecturas por fecha y mercado.
- **Gold `daily_metrics`**: `year/month` — serie temporal.
- **Bronze / Gold `latest` / `brecha`**: sin partición. Son snapshots chicos; partirlos genera carpetas vacías y empeora el listado.

## Watermark (extracción incremental)

DolarAPI no tiene `since=`. El incremental consulta el snapshot y filtra por `fechaActualizacion` contra el estado persistido en:

`data_lake/metadata/extract_watermark.json`

```json
{
  "watermark": "2026-09-05T20:56:00+00:00",
  "watermark_field": "fechaActualizacion",
  "last_run_at": "2026-09-13T15:40:00+00:00",
  "last_mode": "incremental",
  "last_records": 2
}
```

- `watermark`: última fecha de modificación ya procesada.
- `last_run_at`: última ejecución del extractor (aunque no haya habido filas nuevas).

## Modos de escritura Delta

| Modo | Bronze | Silver / Gold |
| --- | --- | --- |
| `--mode full` | `overwrite` del snapshot crudo | overwrite (se regeneran desde Bronze) |
| `--mode incremental` | `MERGE` (upsert por `origen_datos + casa + moneda + fechaActualizacion`). Si Bronze todavía no existe, `overwrite` | overwrite (se regeneran desde Bronze) |

## Configuración PySpark + Delta (`config.py`)

Par de versiones compatible (también en `requirements.txt`): **pyspark 3.5.3 + delta-spark 3.2.1**.

La sesión se arma con `SPARK_DELTA_CONFIG`:

- `spark.sql.extensions` = `io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog` = `org.apache.spark.sql.delta.catalog.DeltaCatalog`

Sin esas dos keys Spark escribe Parquet común y no aparece `_delta_log`.

## Cómo correrlo

Requiere **Java 11 o 17** (lo usa PySpark) y Python 3.9+. Si `java -version` falla, instalá un JDK, definí `JAVA_HOME` si hace falta y reiniciá la terminal.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python main.py --mode full
python main.py --mode incremental
```

## Archivos del trabajo final

```text
config.py           URLs, rutas del lake, SPARK_DELTA_CONFIG
extractors.py       FULL / INCREMENTAL, watermark y validación HTTP/JSON
bronze_layer.py     Load crudo + metadatos + overwrite/MERGE
transform_layer.py  Limpieza PySpark (Bronze → Silver)
gold_layer.py       Persistencia Silver + agregados Gold
spark_session.py    Aplica SPARK_DELTA_CONFIG y crea la sesión
main.py             Orquestador ELT
requirements.txt    pyspark==3.5.3, delta-spark==3.2.1, requests, ...
```

## Fuente

- Base: `https://dolarapi.com`
- [Dólares](https://dolarapi.com/docs/argentina/operations/get-dolares)
- [Cotizaciones](https://dolarapi.com/docs/argentina/operations/get-cotizaciones)

Campos de negocio de la API: `moneda`, `casa`, `nombre`, `compra`, `venta`, `fechaActualizacion`.

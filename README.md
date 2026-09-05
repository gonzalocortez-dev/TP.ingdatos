# Cotizaciones AR — pipeline medallion

Proyecto de ingeniería de datos que ingiere cotizaciones de [DolarAPI](https://dolarapi.com/docs/), las transforma en más de cuatro pasos y las persiste en un data lake local Parquet siguiendo la **arquitectura de medallones** (Bronze / Silver / Gold).

Incluye un flujo **batch** y otro **en tiempo real** (micro-lotes con productor/consumidor). Está pensado para ejecutarse en una máquina local con un solo comando.

## Arquitectura

```text
DolarAPI
  /v1/dolares          ──┐
  /v1/cotizaciones     ──┼──► Bronze (crudo, append-only)
                         │         │
                         │         ▼
                         │      Silver (limpio, tipado, sin duplicados)
                         │         │
                         ▼         ▼
                    Gold: latest | daily_metrics | brecha | fx_cross | tags | variations
```

| Capa | Qué guarda | Particionado | Duplicados |
| --- | --- | --- | --- |
| **Bronze** | JSON original + metadatos de ingestión | `year/month/day/source` (fecha de ingestión) | Se permiten: es la bitácora cruda |
| **Silver** | Hechos limpios, una fila por cotización | `year/month/casa` (fecha de la cotización) | Upsert por `business_key` + `record_hash` |
| **Gold** | Modelos analíticos listos para consumo | Una carpeta por tabla | Se regeneran desde Silver vigente |

Bronze nunca se pisa: cada lote se escribe con un `ingest_id` distinto. Silver es la capa de verdad de negocio. Gold se recalcula para que los reportes no queden desfasados.

## Transformaciones (más de cuatro)

Se aplican en orden, todas vectorizadas con pandas:

1. **Parseo del payload Bronze** — el JSON crudo pasa a columnas.
2. **Normalización de esquema** — `fechaActualizacion` → `fecha_actualizacion`, trim y casing.
3. **Imputación de nulos** — `casa`, `nombre` y `moneda` se completan con valores de negocio.
4. **Casteo de tipos** — precios numéricos y timestamps UTC; fecha faltante → `1900-01-01`.
5. **Columnas de partición** — `year`, `month`, `day` derivadas de la cotización.
6. **Tags de mercado** — clasificación regulado / paralelo / financiero / digital.
7. **Métricas de precio** — `spread`, `mid_price`, `spread_pct`.
8. **Reglas de calidad** — se marcan filas inválidas sin borrarlas (`quality_ok`).
9. **Claves de identidad** — `business_key` y `record_hash`.
10. **Deduplicación** — una fila vigente por clave de negocio.
11. **Filtro analítico** — Gold solo usa cotizaciones válidas.
12. **Unpivot de precios** — formato largo `tipo_precio` / `precio`.
13. **Explode de tags** — una fila por etiqueta de mercado.
14. **Variación inter-tick** — `var_venta_pct` vs la cotización anterior.
15. **Join as-of vs oficial** — brecha de cada USD contra el oficial más reciente.
16. **Agregación diaria** — min / max / promedio / rango de venta.

## Cómo prevenir datos repetidos

- Clave de negocio: `source|casa|moneda|fecha_actualizacion`.
- Hash de contenido: mismos precios y misma fecha → no se vuelve a escribir en Silver.
- En el stream, el consumidor compara el hash contra un índice en memoria + Silver persistido.
- El oficial USD llega por `/v1/dolares` y `/v1/cotizaciones`: en Gold `latest` se elige una sola fuente (USD desde `dolares`, resto desde `cotizaciones`).

## Requisitos

- Python 3.10+
- Conexión a internet para consultar `https://dolarapi.com`

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Opcional: copiá `.env.example` a `.env` para cambiar el directorio del lake o el intervalo del stream.

## Notebook para presentar en Google Colab

Archivo a entregar / abrir: [`notebooks/TP_IngenieriaDatos_Colab.ipynb`](notebooks/TP_IngenieriaDatos_Colab.ipynb)

Es **autocontenido**: trae el cliente, las transformaciones, el lake y los gráficos. No hace falta `src/`, GitHub ni un zip.

1. Entrá a [colab.research.google.com](https://colab.research.google.com).
2. **Archivo → Subir notebook** y elegí `TP_IngenieriaDatos_Colab.ipynb`.
3. **Entorno de ejecución → Ejecutar todas**.

Eso instala dependencias, consulta DolarAPI, arma Bronze/Silver/Gold, grafica y corre 2 micro-lotes en tiempo real.

## Ejecución

Una corrida batch (ingesta + las tres capas):

```powershell
python -m src.cli batch
```

Tiempo real (productor consulta la API, consumidor transforma). Tres ciclos de demo:

```powershell
python -m src.cli stream --interval 20 --cycles 3
```

Stream continuo hasta `Ctrl+C`:

```powershell
python -m src.cli stream --interval 30
```

Estado del lake:

```powershell
python -m src.cli status
```

Pruebas:

```powershell
pytest
```

## Salida del lake

```text
data/lake/
  bronze/quotes/year=2026/month=9/day=5/source=dolares/*.parquet
  silver/quotes/year=2026/month=9/casa=blue/*.parquet
  gold/latest/data.parquet
  gold/daily_metrics/data.parquet
  gold/brecha/data.parquet
  gold/fx_cross/data.parquet
  gold/market_tags/data.parquet
  gold/prices_long/data.parquet
  gold/variations/data.parquet
```

## Estructura del código

```text
src/
  cli.py                  # único punto de entrada
  config.py               # settings desde entorno
  clients/dolar_api.py    # HTTP + reintentos
  transforms/             # Bronze, Silver, Gold y utilidades
  storage/parquet_store.py
  pipelines/batch.py
  pipelines/realtime.py
tests/
notebooks/TP_IngenieriaDatos_Colab.ipynb

```

El cliente etiqueta cada fuente (`dolares`, `cotizaciones`) igual que el script de plataformas etiquetaba `platform_name`. Las utilidades `fill_null_values` y `explode_column` se reutilizan en Silver y Gold.

## Tiempo real

DolarAPI es REST y no expone websocket. El pipeline usa **micro-batch streaming**:

1. Un hilo productor consulta `/v1/dolares` y `/v1/cotizaciones`.
2. Encola el lote.
3. Un hilo consumidor escribe Bronze, descarta hashes ya vistos y actualiza Silver/Gold solo si hubo cambio real.

Es el patrón habitual cuando la fuente es un API de cotizaciones: bajo costo, idempotente y fácil de operar.

## Documentación de la API

- Base: `https://dolarapi.com`
- [Dólares](https://dolarapi.com/docs/argentina/operations/get-dolares)
- [Cotizaciones](https://dolarapi.com/docs/argentina/operations/get-cotizaciones)

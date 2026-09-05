"""Capa Gold: modelos analíticos listos para consumo.

Se construyen varias tablas de negocio a partir de Silver limpio:
latest, métricas diarias, brecha vs oficial, cruce de monedas y tags.
"""

from __future__ import annotations

import pandas as pd

from src.transforms.common import explode_column


def filter_valid_quotes(silver_df: pd.DataFrame) -> pd.DataFrame:
    """T11 — Conserva solo cotizaciones válidas para analítica."""
    if silver_df.empty:
        return silver_df
    return silver_df.loc[silver_df["quality_ok"]].copy()


def unpivot_prices(df: pd.DataFrame) -> pd.DataFrame:
    """T12 — Pasa compra/venta a formato largo (tipo_precio, precio)."""
    id_vars = [
        "source",
        "casa",
        "nombre",
        "moneda",
        "fecha_actualizacion",
        "business_key",
        "year",
        "month",
        "day",
    ]
    return pd.melt(
        df,
        id_vars=id_vars,
        value_vars=["compra", "venta"],
        var_name="tipo_precio",
        value_name="precio",
    )


def explode_market_tags(df: pd.DataFrame) -> pd.DataFrame:
    """T13 — Explota tags de mercado a una fila por etiqueta."""
    cols = [
        "business_key",
        "source",
        "casa",
        "moneda",
        "fecha_actualizacion",
        "market_tags",
    ]
    return explode_column(df, cols, "market_tags", delimiter=",")


def compute_variations(df: pd.DataFrame) -> pd.DataFrame:
    """T14 — Variación vs la cotización anterior de la misma casa/moneda."""
    ordered = df.sort_values(
        ["casa", "moneda", "fecha_actualizacion", "ingest_ts"],
        kind="mergesort",
    )
    grouped = ordered.groupby(["casa", "moneda"], sort=False)
    ordered["venta_anterior"] = grouped["venta"].shift(1)
    ordered["var_venta_pct"] = (
        (ordered["venta"] - ordered["venta_anterior"]) / ordered["venta_anterior"]
    ) * 100.0
    return ordered


def join_oficial_reference(df: pd.DataFrame) -> pd.DataFrame:
    """T15 — Brecha vs el oficial USD más reciente (as-of join)."""
    oficial = (
        df.loc[(df["casa"] == "oficial") & (df["moneda"] == "USD") & (df["source"] == "dolares")]
        .sort_values("fecha_actualizacion")
        .loc[:, ["fecha_actualizacion", "venta", "compra"]]
        .rename(columns={"venta": "venta_oficial", "compra": "compra_oficial"})
    )
    if oficial.empty:
        result = df.copy()
        result["venta_oficial"] = pd.NA
        result["brecha_oficial_pct"] = pd.NA
        return result

    left = df.sort_values("fecha_actualizacion")
    merged = pd.merge_asof(
        left,
        oficial,
        on="fecha_actualizacion",
        direction="backward",
    )
    merged["brecha_oficial_pct"] = pd.Series(pd.NA, index=merged.index, dtype="Float64")
    usd_mask = merged["moneda"] == "USD"
    merged.loc[usd_mask, "brecha_oficial_pct"] = (
        (merged.loc[usd_mask, "venta"] - merged.loc[usd_mask, "venta_oficial"])
        / merged.loc[usd_mask, "venta_oficial"]
    ) * 100.0
    return merged


def build_latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Última cotización vigente por casa y moneda.

    USD se toma de ``dolares``; el resto de monedas, de ``cotizaciones``.
    Así se evita repetir el oficial que llega por ambas fuentes.
    """
    usd = df.loc[(df["moneda"] == "USD") & (df["source"] == "dolares")]
    others = df.loc[df["moneda"] != "USD"]
    combined = pd.concat([usd, others], ignore_index=True)
    combined = combined.sort_values(
        ["casa", "moneda", "fecha_actualizacion", "ingest_ts"],
        ascending=[True, True, False, False],
        kind="mergesort",
    )
    latest = combined.drop_duplicates(subset=["casa", "moneda"], keep="first")
    return latest.reset_index(drop=True)


def aggregate_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """T16 — Agregados diarios: min/max/promedio y primera/última venta."""
    grouped = df.groupby(["year", "month", "day", "casa", "moneda", "source"], as_index=False)
    daily = grouped.agg(
        cotizaciones=("venta", "size"),
        compra_promedio=("compra", "mean"),
        venta_min=("venta", "min"),
        venta_max=("venta", "max"),
        venta_promedio=("venta", "mean"),
        spread_promedio=("spread", "mean"),
        spread_pct_promedio=("spread_pct", "mean"),
        primera_venta=("venta", "first"),
        ultima_venta=("venta", "last"),
    )
    daily["rango_venta"] = daily["venta_max"] - daily["venta_min"]
    return daily


def build_fx_cross(latest: pd.DataFrame) -> pd.DataFrame:
    """Cruce implícito de monedas oficiales contra el USD oficial."""
    usd = latest.loc[(latest["moneda"] == "USD") & (latest["casa"] == "oficial")]
    if usd.empty:
        return pd.DataFrame(
            columns=["moneda", "venta_ars", "usd_oficial_venta", "unidades_por_usd"]
        )

    usd_venta = float(usd.iloc[0]["venta"])
    fx = latest.loc[(latest["casa"] == "oficial") & (latest["moneda"] != "USD")].copy()
    fx["usd_oficial_venta"] = usd_venta
    fx["unidades_por_usd"] = fx["venta"] / usd_venta
    return fx.loc[
        :,
        [
            "moneda",
            "nombre",
            "venta",
            "usd_oficial_venta",
            "unidades_por_usd",
            "fecha_actualizacion",
        ],
    ].rename(columns={"venta": "venta_ars"})


def build_gold_tables(silver_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Orquesta las transformaciones Gold y devuelve el set analítico."""
    valid = filter_valid_quotes(silver_df)
    if valid.empty:
        empty = pd.DataFrame()
        return {
            "latest": empty,
            "daily_metrics": empty,
            "brecha": empty,
            "prices_long": empty,
            "market_tags": empty,
            "fx_cross": empty,
            "variations": empty,
        }

    with_ref = join_oficial_reference(valid)
    variations = compute_variations(with_ref)
    latest = build_latest_snapshot(variations)
    return {
        "latest": latest,
        "daily_metrics": aggregate_daily_metrics(variations),
        "brecha": latest.loc[
            :,
            [
                "casa",
                "nombre",
                "moneda",
                "venta",
                "venta_oficial",
                "brecha_oficial_pct",
                "spread",
                "spread_pct",
                "fecha_actualizacion",
            ],
        ].sort_values("brecha_oficial_pct", na_position="last"),
        "prices_long": unpivot_prices(latest),
        "market_tags": explode_market_tags(latest),
        "fx_cross": build_fx_cross(latest),
        "variations": variations.loc[
            :,
            [
                "casa",
                "moneda",
                "fecha_actualizacion",
                "venta",
                "venta_anterior",
                "var_venta_pct",
                "brecha_oficial_pct",
            ],
        ],
    }

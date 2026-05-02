import pandas as pd
from pathlib import Path

Path("data/silver").mkdir(parents=True, exist_ok=True)
Path("data/gold").mkdir(parents=True, exist_ok=True)


def build_silver() -> pd.DataFrame:
    cdi = pd.read_parquet("data/bronze/cdi.parquet")
    cdi = (cdi
        .dropna(subset=["value"])
        .assign(date=pd.to_datetime(cdi["date"]).dt.normalize())
        .rename(columns={"value": 'taxa_pct_diaria'})
        .drop(columns=["series_code"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    assets = pd.read_parquet('data/bronze/assets.parquet')
    assets = (assets
        .dropna(subset=["close_price"])
        .assign(date=pd.to_datetime(assets["date"]).dt.normalize())
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )

    skeleton_calendar = pd.MultiIndex.from_product([cdi["date"].unique(), assets["ticker"].unique()], names=["date", "ticker"]).to_frame(index=False)
    df = skeleton_calendar.merge(cdi, on="date", how="left")
    df = df.merge(assets, on=["date", "ticker"], how="left")
    df = df.assign(close_price=lambda df: df.groupby("ticker")["close_price"].transform(lambda x: x.ffill()))
    df = df.sort_values(["ticker", "date"])
    return df


def build_gold(silver: pd.DataFrame) -> pd.DataFrame:
    df = silver.sort_values(["ticker", "date"])
    df["daily_return"] = df.groupby("ticker")["close_price"].transform(lambda x: (x / x.shift(1)) - 1)
    for days in [30, 180, 365]:
        df[f"retorno_{days}d"] = df.groupby("ticker")["daily_return"].transform(
            lambda x, d=days: x.rolling(window=d, min_periods=1).apply(
                lambda janela: (1 + janela).prod() - 1
            )
    )
    for days in [30, 180, 365]:
        df[f"cdi_{days}d"] = df.groupby("ticker")["taxa_pct_diaria"].transform(
            lambda x, d=days: x.rolling(window=d, min_periods=1).apply(
                lambda janela: (1 + janela / 100).prod() - 1
            )
        )
        df[f"pct_do_cdi_{days}d"] = df[f"retorno_{days}d"] / df[f"cdi_{days}d"] * 100
    df["drawdown"] = df.groupby("ticker")["close_price"].transform(
        lambda x: x.rolling(window=365, min_periods=1).apply(
            lambda janela: (janela.iloc[-1] / janela.max()) - 1
        )
    )
    print(df[df['date'] == pd.Timestamp("2026-01-30")])
    return df


if __name__ == "__main__":
    print("Construindo Silver...")
    silver = build_silver()
    silver.to_parquet("data/silver/consolidated.parquet", index=False)
    print(f"  ✓ {len(silver)} registros")

    print("Construindo Gold...")
    gold = build_gold(silver)
    gold.to_parquet("data/gold/metrics.parquet", index=False)
    print(f"  ✓ {len(gold)} registros")
    print(f"  Colunas: {gold.columns.tolist()}")

    print(gold.columns.tolist())
    print(gold[gold["ticker"] == "PETR4.SA"].tail(3))
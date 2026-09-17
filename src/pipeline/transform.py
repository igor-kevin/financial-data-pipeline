import pandas as pd
import numpy as np
from pathlib import Path
from prophet import Prophet

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
    # print(df[df['date'] == pd.Timestamp("2026-01-30")])

    df["ma20"] = df.groupby("ticker")["close_price"].transform(
        lambda x: x.rolling(window=20).mean()
    )
    df["std20"] = df.groupby("ticker")["close_price"].transform(
        lambda x: x.rolling(window=20).std()
    )
    df['bb_superior_20'] = df["ma20"] + (2 * df["std20"])
    df['bb_inferior_20'] = df["ma20"] - (2 * df["std20"])

    df["ma50"] = df.groupby("ticker")['close_price'].transform(
        lambda x: x.rolling(window=50).mean()
    )
    df["std50"] = df.groupby("ticker")["close_price"].transform(
        lambda x: x.rolling(window=50).std()
    )
    df['bb_inferior_50'] = df['ma50'] - (2 * df['std50'])
    df['bb_superior_50'] = df['ma50'] + (2 * df['std50'])

    df.drop(columns = ['std20', 'std50'], inplace=True)

    return df


# Função do prophet sem ajuste de dados com log
def prever_aitvo(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    dados = df[df['ticker'] == ticker][['date', 'close_price']]

    dados = dados.rename(columns={'date': 'ds', 'close_price': 'y'})
    
    modelo = Prophet()
    modelo.fit(dados)

    future_df = modelo.make_future_dataframe(periods=30)
    previsao = modelo.predict(future_df)
    previsao['ticker'] = ticker
    previsao['modelo'] = 'prophet'
    
    # print(f'Valor 30: {previsao['yhat'][490:]}')
    return previsao


# Prever ativos com o Prophet e ajuste de log
def prever_aitvo_log(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    dados = df[df['ticker'] == ticker][['date', 'close_price']]

    dados = dados.rename(columns={'date': 'ds', 'close_price': 'y'})
    
    dados['y'] = np.log(dados['y'])
    modelo = Prophet()
    modelo.fit(dados)

    future_df = modelo.make_future_dataframe(periods=30)
    previsao = modelo.predict(future_df)

    previsao['ticker'] = ticker
    previsao['modelo'] = 'prophet_log'

    previsao['yhat'] = np.exp(previsao['yhat'])
    previsao['yhat_lower'] = np.exp(previsao['yhat_lower'])
    previsao['yhat_upper'] = np.exp(previsao['yhat_upper'])
    # print(f'Valor 30: {previsao['yhat'][490:]}')

    return previsao


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
    print(gold[gold['ticker'] == 'ALUP11.SA'].tail(5))

    ativos = []
    print('Prevendo valores dos ativos:')
    for ticker in gold["ticker"].unique():
        print('Ativo atual: ', ticker)
        previsao = prever_aitvo_log(gold, ticker)
        previsao['ticker'] = ticker
        print(' ✓ Valor previsto, iniciando próximo')
        ativos.append(previsao)
    df_ativos = pd.concat(ativos, ignore_index=True)
    colunas_selecionadas = ['ds', 'ticker', 'yhat','yhat_lower', 'yhat_upper', 'modelo']
    df_ativos = df_ativos[colunas_selecionadas]
    print(f' ✓ Previsoes completas: \n {df_ativos.tail(3)}')

    df_ativos = df_ativos.rename(columns={
                                    'ds': 'date',
                                    'yhat': 'previsao',
                                    'yhat_lower': 'previsao_min',
                                    'yhat_upper': 'previsao_max'})
    ultima_data = gold.groupby('ticker')['date'].max()
    futuros = []
    for ticker in df_ativos['ticker'].unique():
        mask = df_ativos['ticker'] == ticker
        mask_data = df_ativos['date'] > pd.Timestamp(ultima_data[ticker])
        futuros.append(df_ativos[mask & mask_data])

    df_futuros = pd.concat(futuros, ignore_index=True)
    df_futuros.to_parquet('data/gold/forecast.parquet', index=False)
    print(f'Registros por ticker: {df_futuros.groupby('ticker').size()}')

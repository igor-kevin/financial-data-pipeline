import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

Path("data/bronze").mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────

SERIES_BCB = {
    "cdi":   12,
    "selic": 11,
    "ipca":  433,
}

TICKERS = ["^BVSP", "PETR4.SA", "VALE3.SA", "MXRF11.SA", 'ALUP11.SA', 'ONCO3.SA']


# ─────────────────────────────────────────
# BANCO CENTRAL
# ─────────────────────────────────────────

def extract_bcb(series_code: int, start_date: str, end_date: str) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series_code"] = series_code
    return df


def run_bcb():
    print("Extraindo séries do Banco Central...")
    end = datetime.today().strftime("%d/%m/%Y")
    start = (datetime.today() - timedelta(days=730)).strftime("%d/%m/%Y")

    for name, code in SERIES_BCB.items():
        print(f"  Extraindo {name.upper()} (série {code})...")
        df = extract_bcb(code, start, end)
        output_path = f"data/bronze/{name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"  ✓ {len(df)} registros salvos em {output_path}")


# ─────────────────────────────────────────
# ATIVOS FINANCEIROS
# ─────────────────────────────────────────

def extract_assets(tickers: list, period: str = "2y") -> pd.DataFrame:
    frames = []
    for ticker in tickers:
        print(f"  Extraindo {ticker}...")
        try:
            df = yf.download(ticker, period=period, auto_adjust=True, progress=False)

            if df.empty:
                print(f"  ⚠ {ticker}: sem dados, pulando.")
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df[["Close"]].copy()
            df.columns = ["close_price"]
            df.index.name = "date"
            df = df.reset_index()
            df["ticker"] = ticker
            df["date"] = pd.to_datetime(df["date"])
            frames.append(df)

        except Exception as e:
            print(f"  ✗ Erro ao extrair {ticker}: {e}")

    if not frames:
        raise RuntimeError("Nenhum ativo foi extraído com sucesso.")

    return pd.concat(frames, ignore_index=True)


def run_assets():
    print("\nExtraindo ativos financeiros...")
    df = extract_assets(TICKERS)
    output_path = "data/bronze/assets.parquet"
    df.to_parquet(output_path, index=False)
    print(f"✓ {len(df)} registros salvos em {output_path}")
    print(f"  Ativos: {df['ticker'].unique().tolist()}")
    print(f"  Período: {df['date'].min().date()} → {df['date'].max().date()}")


# ─────────────────────────────────────────
# EXECUÇÃO
# ─────────────────────────────────────────

if __name__ == "__main__":
    run_bcb()
    run_assets()
    print("\n✅ Extração completa. Camada Bronze pronta.")
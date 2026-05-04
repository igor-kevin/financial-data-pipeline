import pandas as pd
import psycopg
from dotenv import load_dotenv
import os

load_dotenv()
print(f"USER: {os.getenv('DB_USER')}")
print(f"PASS: {os.getenv('DB_PASSWORD')}")
print(f"HOST: {os.getenv('DB_HOST')}")
print(f"PORT: {os.getenv('DB_PORT')}")
print(f"NAME: {os.getenv('DB_NAME')}")

def new_conn():
    print('conectando')
    return psycopg.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


def load_gold_to_postgres():
    df = pd.read_parquet("data/gold/metrics.parquet")

    colunas = [ 
        'date', 'ticker', 'taxa_pct_diaria', 'close_price', 'daily_return',
        'retorno_30d', 'retorno_180d', 'retorno_365d',
        'cdi_30d', 'pct_do_cdi_30d',
        'cdi_180d', 'pct_do_cdi_180d',
        'cdi_365d', 'pct_do_cdi_365d',
        'drawdown'

    ]
    df = df[colunas]
    df = df.replace({float('nan'): None})

    print(df)
    records = list(df.itertuples(index=False, name=None))
    con = new_conn()
    cursor = con.cursor()
    query = '''
        INSERT INTO financial_data  (
            date, ticker, taxa_pct_diaria, close_price, daily_return,
            retorno_30d, retorno_180d, retorno_365d,
            cdi_30d, pct_do_cdi_30d,
            cdi_180d, pct_do_cdi_180d,
            cdi_365d, pct_do_cdi_365d,
            drawdown
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, ticker) DO NOTHING;
    '''
    cursor.executemany(query, records)
    con.commit()
    print(f' {cursor.rowcount} registros foram inseridos com sucesso')
    cursor.close()
    con.close()



if __name__ == '__main__':
    conexao = new_conn()
    print('ok')
    load_gold_to_postgres()
    print('golded')

    cur = conexao.cursor()
    query = """
        SELECT
            ticker,
            count(*) AS registro,
            COUNT(pct_do_cdi_365d) AS nao_nulos,
            COUNT(*) - COUNT(pct_do_cdi_365d) as nulos,
            ROUND(AVG(pct_do_cdi_365d)::numeric, 2),
            ROUND(MIN(drawdown)::numeric, 4) AS pior_drawdown
        FROM
            financial_data
        GROUP BY
            ticker
        ORDER BY
            ticker
    """
    out = cur.execute(query).fetchall()
    df_out = pd.DataFrame(out)
    print(df_out)
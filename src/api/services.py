from psycopg.rows import dict_row
from ..core.db import get_conn
from .schemas import TickerMetric, TickerCompare, TickerResume
from typing import List

def get_tickers() -> List[str]:
    query_tickers = """
        SELECT DISTINCT
            ticker
        FROM 
           financial_data
        WHERE 
            ticker != '^BVSP'
        ORDER BY
            ticker
        """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_tickers)
            rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_metricas(ticker: str) -> List[TickerMetric]:
    query_metricas = """
        SELECT 
            date,
            ticker,
            close_price,
            daily_return,
            retorno_30d,
            retorno_180d,
            retorno_365d,
            pct_do_cdi_30d,
            pct_do_cdi_180d,
            pct_do_cdi_365d,
            drawdown
        FROM 
            financial_data
        WHERE 
            ticker = %s
        ORDER BY date
    """
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query_metricas, (ticker,))

            rows = cursor.fetchall()

    return [TickerMetric(**row) for row in rows]


def get_comparativo(ticker: str) -> list:
    query_comparativo = '''
        SELECT 
            a.date,
            a.ticker,
            a.retorno_365d,
            a.cdi_365d,
            b.retorno_365d AS ibov_365d
        FROM financial_data a
        JOIN financial_data b 
            ON a.date = b.date 
            AND b.ticker = '^BVSP'
        WHERE a.ticker = %s
        ORDER BY a.date
    '''
    args = ticker
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            comparativo = cursor.execute(query_comparativo, (args,)).fetchall()

    return [TickerCompare(**row) for row in comparativo]


def get_resumo() -> list:
    query_resumo = '''
        SELECT DISTINCT on (ticker)
            ticker,
            pct_do_cdi_365d,
            drawdown,
            retorno_30d,
            retorno_365d,
            close_price
        FROM
            financial_data
        ORDER BY 
            ticker, date DESC


    '''
    with get_conn() as conexao:
        with conexao.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query_resumo)
            resumo = cursor.fetchall()
    return [TickerResume(**row) for row in resumo] 

if __name__ == '__main__':
    print(get_tickers())
    print(get_metricas('ALUP11.SA'))
    print(get_comparativo('ALUP11.SA'))
    print(get_resumo())
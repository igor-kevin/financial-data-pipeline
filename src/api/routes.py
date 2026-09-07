from fastapi import APIRouter, HTTPException
from .services import get_tickers, get_metricas, get_comparativo, get_resumo, get_bollinger
from .schemas import TickerCompare, TickerMetric, TickerResume, BollingerItem
from typing import List


router = APIRouter()


@router.get("/ticker", response_model=List[str])
def tickers():
    return get_tickers()


@router.get("/metricas/{ticker}", response_model=List[TickerMetric] )
def metricas(ticker: str):
    dados = get_metricas(ticker)
    if not dados:
        raise HTTPException(status_code=404, detail='Ticker não encontrado')
    return dados


@router.get("/comparativo/{ticker}", response_model=List[TickerCompare])
def comparativo(ticker: str):
    compara = get_comparativo(ticker)
    if not compara:
        raise HTTPException(status_code=404, detail='Ticker não encontrado')
    return compara


@router.get("/resumo", response_model=List[TickerResume])
def resumo():
    return get_resumo()


@router.get('/bollinger/{ticker}', response_model=list[BollingerItem])
def bollinger(ticker: str):
    dados = get_bollinger(ticker)
    if not dados:
        raise HTTPException(status_code=404, detail='Ticker não encontrado')
    return dados

@router.get("/health")
def health():
    return {"status": "ok"}
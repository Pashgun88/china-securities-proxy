"""
forecast_endpoints.py

Роутер с эндпоинтами прогнозов консенсуса аналитиков для FastAPI-прокси на AKShare.
Подключается к существующему приложению одной строкой (см. main.py).

Источники:
  /forecast/em/{symbol}        -> Eastmoney (东财), только A-акции. Сам akshare-интерфейс
                                   фильтрует только по отраслевому сегменту, а не по тикеру,
                                   поэтому фильтрация по коду делается на стороне прокси.
  /forecast/ths/{symbol}       -> Tonghuashun / 同花顺, только A-акции.
  /forecast/hk/{symbol}        -> ET Net (经济通), гонконгские тикеры (BOC/CCB/CITIC и т.д.).
  /forecast/aggregate/{symbol} -> собирает то, что применимо к данному коду, в один JSON.
"""

import os
import time
from typing import Optional

import akshare as ak
import pandas as pd
from fastapi import APIRouter, Header, HTTPException, Query

router = APIRouter(prefix="/forecast", tags=["forecast"])

PROXY_ACCESS_KEY = os.environ.get("PROXY_ACCESS_KEY")


def check_auth(authorization: Optional[str] = Header(default=None)) -> None:
    if not PROXY_ACCESS_KEY:
        return
    expected = f"Bearer {PROXY_ACCESS_KEY}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Простой TTL-кэш в памяти, чтобы не гонять пагинацию EM (постранично, склеивается
# внутри akshare) при каждом запросе. Кэш общий на процесс — этого достаточно для
# одного инстанса на Render.
_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 часов
_em_cache: dict = {"data": None, "ts": 0.0}


def _df_to_records(df: pd.DataFrame) -> list:
    df = df.astype(object).where(df.notnull(), None)
    df = df.replace([float("inf"), float("-inf")], None)
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# 1. Eastmoney (东财) — A-акции
# ---------------------------------------------------------------------------

def _get_em_full_table() -> pd.DataFrame:
    """
    stock_profit_forecast_em не фильтрует по тикеру — только по отраслевому
    сегменту. Поэтому тянем всю таблицу целиком и кэшируем, а фильтрацию по
    конкретному коду делаем уже на своей стороне.
    """
    now = time.time()
    if _em_cache["data"] is not None and (now - _em_cache["ts"]) < _CACHE_TTL_SECONDS:
        return _em_cache["data"]

    df = ak.stock_profit_forecast_em(symbol="")
    _em_cache["data"] = df
    _em_cache["ts"] = now
    return df


@router.get("/em/{symbol}")
def forecast_em(symbol: str, authorization: Optional[str] = Header(default=None)):
    """Консенсус-прогноз Eastmoney по A-акции. symbol без биржевого суффикса, напр. "601939"."""
    check_auth(authorization)
    try:
        df = _get_em_full_table()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Eastmoney fetch failed: {e}")

    if "代码" not in df.columns:
        raise HTTPException(status_code=502, detail="Unexpected EM response schema (no '代码' column)")

    match = df[df["代码"].astype(str).str.zfill(6) == symbol.zfill(6)]
    if match.empty:
        return {
            "source": "eastmoney",
            "symbol": symbol,
            "found": False,
            "note": "не найдено — нет покрытия аналитиками EM либо код не A-share",
            "data": [],
        }

    return {
        "source": "eastmoney",
        "symbol": symbol,
        "found": True,
        "as_of_cache_ts": _em_cache["ts"],
        "data": _df_to_records(match),
    }


# ---------------------------------------------------------------------------
# 2. Tonghuashun / 同花顺 — A-акции
# ---------------------------------------------------------------------------

_THS_INDICATORS = [
    "预测年报每股收益",
    "预测年报净利润",
    "业绩预测详表-机构",
    "业绩预测详表-详细指标预测",
]


@router.get("/ths/{symbol}")
def forecast_ths(
    symbol: str,
    indicator: str = Query(
        "业绩预测详表-详细指标预测",
        description=f"Один из: {_THS_INDICATORS}",
    ),
    authorization: Optional[str] = Header(default=None),
):
    """
    Консенсус-прогноз Tonghuashun (同花顺) по A-акции. symbol без суффикса, напр. "601939".
    indicator должен передаваться ТОЧНО одной из строк из _THS_INDICATORS — частичные/
    произвольные значения у THS иногда возвращают пустой ответ.
    """
    check_auth(authorization)
    if indicator not in _THS_INDICATORS:
        raise HTTPException(
            status_code=400,
            detail=f"indicator must be exactly one of {_THS_INDICATORS}",
        )
    try:
        df = ak.stock_profit_forecast_ths(symbol=symbol, indicator=indicator)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"THS fetch failed: {e}")

    if df is None or df.empty:
        return {
            "source": "10jqka",
            "symbol": symbol,
            "indicator": indicator,
            "found": False,
            "note": "не найдено — либо нет покрытия, либо неверный indicator",
            "data": [],
        }

    return {
        "source": "10jqka",
        "symbol": symbol,
        "indicator": indicator,
        "found": True,
        "data": _df_to_records(df),
    }


# ---------------------------------------------------------------------------
# 3. ET Net / 经济通 — гонконгские акции (основной источник для BOC/CCB/CITIC)
# ---------------------------------------------------------------------------

_ET_INDICATORS = ["评级总览", "去年度业绩表现", "综合盈利预测", "盈利预测概览"]


@router.get("/hk/{symbol}")
def forecast_hk(
    symbol: str,
    indicator: str = Query("综合盈利预测", description=f"Один из: {_ET_INDICATORS}"),
    authorization: Optional[str] = Header(default=None),
):
    """
    Консенсус-прогноз ET Net (经济通) по гонконгской акции.
    symbol: код HKEX, akshare сам паддит до 5 знаков — можно передавать "939", "0939" или "00939".
    """
    check_auth(authorization)
    if indicator not in _ET_INDICATORS:
        raise HTTPException(
            status_code=400, detail=f"indicator must be exactly one of {_ET_INDICATORS}"
        )
    hk_symbol = symbol.zfill(5)
    try:
        df = ak.stock_hk_profit_forecast_et(symbol=hk_symbol, indicator=indicator)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ET Net fetch failed: {e}")

    if df is None or df.empty:
        return {
            "source": "etnet",
            "symbol": hk_symbol,
            "indicator": indicator,
            "found": False,
            "note": "не найдено — нет покрытия аналитиками либо неверный код",
            "data": [],
        }

    return {
        "source": "etnet",
        "symbol": hk_symbol,
        "indicator": indicator,
        "found": True,
        "data": _df_to_records(df),
    }


# ---------------------------------------------------------------------------
# 4. Агрегатор — собирает применимые источники по одному коду
# ---------------------------------------------------------------------------

@router.get("/aggregate/{symbol}")
def forecast_aggregate(
    symbol: str,
    hk_symbol: Optional[str] = Query(
        None, description="Если у бумаги есть отдельный HK-код, отличный от symbol"
    ),
    authorization: Optional[str] = Header(default=None),
):
    """
    Возвращает все применимые источники по одному вызову — удобно, чтобы не делать
    по три отдельных запроса на каждую компанию.

    Логика:
      - EM и THS дёргаются, если symbol похож на A-share (6 цифр);
      - ET Net дёргается, если передан hk_symbol (или symbol как есть, если сам
        symbol уже гонконгский, напр. "0939").
    """
    check_auth(authorization)
    results = {"symbol": symbol, "sources": {}}

    is_a_share_like = symbol.isdigit() and len(symbol) == 6

    if is_a_share_like:
        try:
            results["sources"]["eastmoney"] = forecast_em(symbol, authorization)
        except HTTPException as e:
            results["sources"]["eastmoney"] = {"error": e.detail}
        try:
            results["sources"]["10jqka"] = forecast_ths(symbol, authorization=authorization)
        except HTTPException as e:
            results["sources"]["10jqka"] = {"error": e.detail}

    hk_code = hk_symbol or (symbol if not is_a_share_like else None)
    if hk_code:
        try:
            results["sources"]["etnet"] = forecast_hk(hk_code, authorization=authorization)
        except HTTPException as e:
            results["sources"]["etnet"] = {"error": e.detail}

    if not results["sources"]:
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить ни один применимый источник для этого symbol. "
            "Укажите hk_symbol явно, если это гонконгская бумага.",
        )

    return results

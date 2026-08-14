import os
import re
from datetime import datetime, timezone
from typing import Optional

import akshare as ak
import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from broker_consensus import fetch_aastocks_consensus
from errors import UpstreamError, cached_call, call_akshare_with_retry, classify_exception
from forecast_endpoints import router as forecast_router

app = FastAPI(title="China Securities Data Proxy")
app.include_router(forecast_router)


@app.exception_handler(UpstreamError)
async def upstream_error_handler(request: Request, exc: UpstreamError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "error_type": exc.error_type,
            "endpoint": request.url.path,
            "message": exc.message,
            "retryable": exc.retryable,
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Страховка для любых необработанных исключений (не только из akshare)."""
    error_type, status_code, retryable = classify_exception(exc)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": True,
            "error_type": error_type,
            "endpoint": request.url.path,
            "message": str(exc)[:300],
            "retryable": retryable,
        },
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROXY_ACCESS_KEY = os.environ.get("PROXY_ACCESS_KEY")


def check_auth(authorization: Optional[str] = Header(default=None)) -> None:
    if not PROXY_ACCESS_KEY:
        return
    expected = f"Bearer {PROXY_ACCESS_KEY}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def clean_ticker(ts_code: str) -> str:
    code = ts_code.strip().upper()
    code = re.sub(r"\.(SH|SZ|HK)$", "", code)
    code = code.replace(".", "")
    return code


def clean_hk_ticker(ts_code: str) -> str:
    """
    HK-эндпоинты akshare (stock_financial_hk_report_em, stock_hk_hist) матчат
    тикер по точному 5-значному коду с ведущими нулями (напр. "00175") —
    без паддинга запрос по коду вида "175"/"0175" не находит ничего у
    Eastmoney и падает с TypeError при разборе пустого result, а не с
    осмысленной ошибкой "не найдено".
    """
    return clean_ticker(ts_code).zfill(5)


def df_response(df: Optional[pd.DataFrame]) -> dict:
    if df is None or df.empty:
        return {"data": [], "note": "не найдено — источник вернул пустой результат"}
    df = df.astype(object).where(df.notnull(), None)
    df = df.replace([float("inf"), float("-inf")], None)
    return {"data": df.to_dict(orient="records")}


def call_akshare(func, **kwargs):
    return call_akshare_with_retry(func, **kwargs)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/income")
def income(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(ak.stock_financial_report_sina, stock=code, symbol="利润表")
    return df_response(df)


@app.get("/balancesheet")
def balancesheet(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(ak.stock_financial_report_sina, stock=code, symbol="资产负债表")
    return df_response(df)


@app.get("/cashflow")
def cashflow(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(ak.stock_financial_report_sina, stock=code, symbol="现金流量表")
    return df_response(df)


@app.get("/dividend")
def dividend(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    func = getattr(ak, "stock_history_dividend_detail", None)
    if func is None:
        func = getattr(ak, "stock_fhps_detail_em")
        df = call_akshare(func, symbol=code)
    else:
        df = call_akshare(func, symbol=code, indicator="分红")
    return df_response(df)


@app.get("/daily")
def daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(
        ak.stock_zh_a_hist,
        symbol=code,
        period="daily",
        start_date=start_date.replace("-", ""),
        end_date=end_date.replace("-", ""),
        adjust="qfq",
    )
    return df_response(df)


_HK_RESAMPLE_RULE = {"weekly": "W", "monthly": "ME", "yearly": "YE"}


def _resample_hk_daily(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    agg = df.resample(_HK_RESAMPLE_RULE[interval]).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", "amount": "sum"}
    )
    agg = agg.dropna(subset=["open"]).reset_index()
    agg["date"] = agg["date"].dt.strftime("%Y-%m-%d")
    return agg


@app.get("/hk_daily")
def hk_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    interval: str = Query(
        "daily", description="daily (по умолчанию) | weekly | monthly | yearly — агрегация OHLCV"
    ),
    authorization: Optional[str] = Header(default=None),
):
    check_auth(authorization)
    if interval not in ("daily", "weekly", "monthly", "yearly"):
        raise HTTPException(status_code=400, detail="interval must be one of: daily, weekly, monthly, yearly")
    code = clean_hk_ticker(ts_code)
    # ak.stock_hk_hist (Eastmoney kline API, 33.push2his.eastmoney.com) регулярно
    # обрывает соединение без ответа — как с датацентровых IP (Render), так и
    # локально, независимо от заголовков/повторов. ak.stock_hk_daily (Sina)
    # отдаёт ту же дневную историю надёжно, но не принимает диапазон дат —
    # тянем всю историю и фильтруем на своей стороне. Кэшируем full-history
    # результат на 30 мин на тикер — меньше живых обращений к апстриму и
    # быстрее повторные вопросы про один и тот же тикер в рамках диалога.
    df = cached_call(f"hk_daily:{code}", 1800, ak.stock_hk_daily, symbol=code, adjust="qfq")
    if df is not None and not df.empty:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        if interval != "daily" and not df.empty:
            df = _resample_hk_daily(df, interval)
    if df is not None and not df.empty:
        df = df.round(4)
    return df_response(df)


@app.get("/hk_financial")
def hk_financial(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_hk_ticker(ts_code)
    # Финансовая отчётность не меняется внутри дня — кэш на 6ч (как EM-кэш
    # в forecast_endpoints.py) снижает число живых обращений к апстриму.
    df = cached_call(
        f"hk_financial:{code}",
        6 * 3600,
        ak.stock_financial_hk_report_em,
        stock=code,
        symbol="资产负债表",
        indicator="年度",
    )
    return df_response(df)


@app.get("/fx")
def fx(
    currency: str = Query(default="港币"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    authorization: Optional[str] = Header(default=None),
):
    check_auth(authorization)
    kwargs = {"symbol": currency}
    if start_date:
        kwargs["start_date"] = start_date.replace("-", "")
    if end_date:
        kwargs["end_date"] = end_date.replace("-", "")
    df = call_akshare(ak.currency_boc_sina, **kwargs)
    return df_response(df)


@app.get("/fx_cny_hkd_on_date")
def fx_cny_hkd_on_date(date: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    d = date.replace("-", "")
    df = call_akshare(ak.currency_boc_sina, symbol="港币", start_date=d, end_date=d)
    result = df_response(df)
    result["source"] = "Bank of China (BOC) official quote via AKShare currency_boc_sina"
    result["methodology"] = "ПРЯМАЯ котировка, не кросс-расчёт"
    return result


@app.get("/stock_basic")
def stock_basic(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(ak.stock_individual_info_em, symbol=code)
    return df_response(df)


@app.get("/date")
def server_date():
    """
    Текущая дата сервера. Нужна GPT: собственных часов у модели нет, а дата в
    её контексте может расходиться с реальной — без этого она принимает
    прогнозы на уже прошедшие годы за актуальные. Авторизация не требуется:
    ничего чувствительного не отдаёт, а вызывается перед каждым разбором дат.
    """
    now = datetime.now(timezone.utc)
    return {
        "today": now.date().isoformat(),
        "year": now.year,
        "datetime_utc": now.isoformat(timespec="seconds"),
    }


@app.get("/consensus/aastocks/{symbol}")
def consensus_aastocks(symbol: str, authorization: Optional[str] = Header(default=None)):
    """
    Manual/low-frequency use only — not for automated polling. Парсит
    research-заметки AASTOCKS.com по HK-тикеру (ожидается 1-2 запуска в год).
    НЕ подключён к keep-warm workflow и не должен вызываться автоматически.
    """
    check_auth(authorization)
    return fetch_aastocks_consensus(symbol)

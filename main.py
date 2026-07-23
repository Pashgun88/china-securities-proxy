import os
import re
from typing import Optional

import akshare as ak
import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="China Securities Data Proxy")

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


def df_response(df: Optional[pd.DataFrame]) -> dict:
    if df is None or df.empty:
        return {"data": [], "note": "не найдено — источник вернул пустой результат"}
    df = df.astype(object).where(df.notnull(), None)
    df = df.replace([float("inf"), float("-inf")], None)
    return {"data": df.to_dict(orient="records")}


def call_akshare(func, **kwargs):
    try:
        return func(**kwargs)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


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


@app.get("/hk_daily")
def hk_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(
        ak.stock_hk_hist,
        symbol=code,
        period="daily",
        start_date=start_date.replace("-", ""),
        end_date=end_date.replace("-", ""),
        adjust="qfq",
    )
    return df_response(df)


@app.get("/hk_financial")
def hk_financial(ts_code: str = Query(...), authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    code = clean_ticker(ts_code)
    df = call_akshare(ak.stock_financial_hk_report_em, stock=code, symbol="资产负债表", indicator="年度")
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

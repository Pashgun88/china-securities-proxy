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
import re
import time
from datetime import date, datetime, timezone
from typing import Optional

import akshare as ak
import pandas as pd
from fastapi import APIRouter, Header, HTTPException, Query

from errors import UpstreamError, call_akshare_with_retry

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


def _now_iso() -> str:
    """Дата получения данных — чтобы GPT цитировал её, а не выдумывал актуальность."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _current_year() -> int:
    return datetime.now(timezone.utc).year


def _to_jsonable(value):
    # ET Net отдаёт 更新日期 объектами date — json.dumps на них падает, поэтому
    # приводим любые даты/таймстемпы к ISO-строке до сериализации.
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    return value


def _df_to_records(df: pd.DataFrame) -> list:
    df = df.astype(object).where(df.notnull(), None)
    df = df.replace([float("inf"), float("-inf")], None)
    return [{k: _to_jsonable(v) for k, v in row.items()} for row in df.to_dict(orient="records")]


# Год в ответах источников встречается в двух видах, поэтому фильтруем оба:
#   - строкой (ET Net "财政年度", THS "年度") — отбрасываем строки прошлых лет;
#   - в имени колонки (EM "2025预测每股收益", THS "2023-实际值") — отбрасываем колонки.
_PERIOD_COLUMNS = ("财政年度", "年度", "报告年度")
_YEAR_IN_NAME = re.compile(r"(19|20)\d{2}")


def _drop_past_periods(df: pd.DataFrame) -> tuple:
    """
    Оставляет только периоды текущего года и позже. Возвращает (df, dropped),
    где dropped — список отброшенных периодов, чтобы ответ оставался прозрачным
    и было видно, что данные урезаны, а не отсутствуют у источника.
    """
    if df is None or df.empty:
        return df, []

    year = _current_year()
    dropped = []

    period_col = next((c for c in _PERIOD_COLUMNS if c in df.columns), None)
    if period_col is not None:
        years = pd.to_numeric(df[period_col], errors="coerce")
        # NaN (нераспознанный период) сохраняем: лучше лишняя строка, чем молча потерянная.
        keep = years.isna() | (years >= year)
        dropped = [str(v) for v in df.loc[~keep, period_col].tolist()]
        df = df[keep]

    stale_cols = []
    for col in df.columns:
        match = _YEAR_IN_NAME.search(str(col))
        if match and int(match.group(0)) < year:
            stale_cols.append(col)
    if stale_cols:
        dropped.extend(str(c) for c in stale_cols)
        df = df.drop(columns=stale_cols)

    return df, dropped


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

    df = call_akshare_with_retry(ak.stock_profit_forecast_em, symbol="")
    _em_cache["data"] = df
    _em_cache["ts"] = now
    return df


def _em_cache_iso() -> Optional[str]:
    """Для EM данные могут быть из кэша — отдаём время их фактической загрузки."""
    if not _em_cache["ts"]:
        return None
    return datetime.fromtimestamp(_em_cache["ts"], timezone.utc).isoformat(timespec="seconds")


@router.get("/em/{symbol}")
def forecast_em(
    symbol: str,
    include_past: bool = Query(
        False, description="true — вернуть также прогнозы на уже прошедшие годы"
    ),
    authorization: Optional[str] = Header(default=None),
):
    """Консенсус-прогноз Eastmoney по A-акции. symbol без биржевого суффикса, напр. "601939"."""
    check_auth(authorization)
    df = _get_em_full_table()

    if "代码" not in df.columns:
        raise UpstreamError(
            "upstream_response_changed",
            502,
            "Unexpected EM response schema (no '代码' column)",
            False,
        )

    match = df[df["代码"].astype(str).str.zfill(6) == symbol.zfill(6)]
    if match.empty:
        return {
            "source": "eastmoney",
            "symbol": symbol,
            "today": _today(),
            "retrieved_at": _em_cache_iso(),
            "found": False,
            "note": "не найдено — нет покрытия аналитиками EM либо код не A-share",
            "data": [],
        }

    dropped = []
    if not include_past:
        match, dropped = _drop_past_periods(match)

    return {
        "source": "eastmoney",
        "symbol": symbol,
        "found": True,
        "today": _today(),
        "retrieved_at": _em_cache_iso(),
        "as_of_cache_ts": _em_cache["ts"],
        "dropped_past_periods": dropped,
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
_THS_DEFAULT_INDICATOR = "业绩预测详表-详细指标预测"


@router.get("/ths/{symbol}")
def forecast_ths(
    symbol: str,
    indicator: str = Query(
        _THS_DEFAULT_INDICATOR,
        description=f"Один из: {_THS_INDICATORS}",
    ),
    include_past: bool = Query(
        False, description="true — вернуть также прогнозы/факт на уже прошедшие годы"
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
    df = call_akshare_with_retry(ak.stock_profit_forecast_ths, symbol=symbol, indicator=indicator)

    if df is None or df.empty:
        return {
            "source": "10jqka",
            "symbol": symbol,
            "indicator": indicator,
            "today": _today(),
            "retrieved_at": _now_iso(),
            "found": False,
            "note": "не найдено — либо нет покрытия, либо неверный indicator",
            "data": [],
        }

    dropped = []
    if not include_past:
        df, dropped = _drop_past_periods(df)

    return {
        "source": "10jqka",
        "symbol": symbol,
        "indicator": indicator,
        "today": _today(),
        "retrieved_at": _now_iso(),
        "found": True,
        "dropped_past_periods": dropped,
        "data": _df_to_records(df),
    }


# ---------------------------------------------------------------------------
# 3. ET Net / 经济通 — гонконгские акции (основной источник для BOC/CCB/CITIC)
# ---------------------------------------------------------------------------

_ET_INDICATORS = ["评级总览", "去年度业绩表现", "综合盈利预测", "盈利预测概览"]
_ET_DEFAULT_INDICATOR = "综合盈利预测"


@router.get("/hk/{symbol}")
def forecast_hk(
    symbol: str,
    indicator: str = Query(_ET_DEFAULT_INDICATOR, description=f"Один из: {_ET_INDICATORS}"),
    include_past: bool = Query(
        False, description="true — вернуть также прогнозы на уже прошедшие финансовые годы"
    ),
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
    df = call_akshare_with_retry(ak.stock_hk_profit_forecast_et, symbol=hk_symbol, indicator=indicator)

    if df is None or df.empty:
        return {
            "source": "etnet",
            "symbol": hk_symbol,
            "indicator": indicator,
            "today": _today(),
            "retrieved_at": _now_iso(),
            "found": False,
            "note": "не найдено — нет покрытия аналитиками либо неверный код",
            "data": [],
        }

    dropped = []
    if not include_past:
        df, dropped = _drop_past_periods(df)

    return {
        "source": "etnet",
        "symbol": hk_symbol,
        "indicator": indicator,
        "today": _today(),
        "retrieved_at": _now_iso(),
        "found": True,
        "dropped_past_periods": dropped,
        "data": _df_to_records(df),
    }


# ---------------------------------------------------------------------------
# 3a. Индивидуальные брокерские прогнозы (ET Net 盈利预测概览)
# ---------------------------------------------------------------------------
#
# В отличие от 综合盈利预测 (агрегат без имён), этот индикатор даёт provenance:
# по каждой строке видно, какой дом, когда и что именно спрогнозировал —
#   财政年度 | 纯利/亏损 | 每股盈利 | 每股派息 | 证券商 | 评级 | 目标价 | 更新日期
# Именно это нужно, чтобы не работать с обезличенным консенсусом.

_BROKER_INDICATOR = "盈利预测概览"
_COL_BROKER = "证券商"
_COL_DATE = "更新日期"
_COL_FY = "财政年度"
_COL_EPS = "每股盈利"

# Ступени расширения окна, если в базовом окне не набралось min_brokers.
_WINDOW_LADDER_DAYS = [60, 90, 180, 365]

# Меньше трёх точек — разброс считать не на чем: любое из двух значений
# формально «вдвое дальше» второго, и разметка выбросов теряет смысл.
_MIN_POINTS_FOR_OUTLIERS = 3


def _mark_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Размечает выбросы по правилу заказчика, отдельно внутри каждого финансового
    года: прогноз считается выбросом, если его отклонение от среднего более чем
    вдвое превышает следующее по величине отклонение среди остальных.

    Строки НЕ удаляются: whitelist «известных домов» отсутствует, поэтому
    решение, учитывать ли выброс, принимает человек — у него в строке есть имя
    брокера и дата.
    """
    df = df.copy()
    df["is_outlier"] = False
    if _COL_EPS not in df.columns or _COL_FY not in df.columns:
        return df

    for fy, group in df.groupby(_COL_FY):
        eps = pd.to_numeric(group[_COL_EPS], errors="coerce").dropna()
        if len(eps) < _MIN_POINTS_FOR_OUTLIERS:
            continue
        deviations = (eps - eps.mean()).abs().sort_values(ascending=False)
        top, runner_up = deviations.iloc[0], deviations.iloc[1]
        if runner_up > 0 and top > 2 * runner_up:
            df.loc[deviations.index[0], "is_outlier"] = True

    return df


@router.get("/hk_brokers/{symbol}")
def forecast_hk_brokers(
    symbol: str,
    max_age_days: int = Query(30, ge=1, description="Базовое окно свежести прогнозов, дней"),
    min_brokers: int = Query(
        10, ge=1, description="Сколько уникальных брокеров должно набраться в окне"
    ),
    include_past: bool = Query(
        False, description="true — оставить прогнозы на уже прошедшие финансовые годы"
    ),
    authorization: Optional[str] = Header(default=None),
):
    """
    Индивидуальные прогнозы брокеров по гонконгской акции: кто, когда и какой
    прогноз дал — в отличие от обезличенного консенсуса /forecast/hk.

    Окно свежести: берём max_age_days; если уникальных брокеров меньше
    min_brokers — расширяем окно по ступеням и в пределе берём всё, что есть.
    Считаем именно уникальных брокеров, а не строки: один дом даёт по строке на
    каждый финансовый год, и по строкам порог набирается ложно.

    Если покрытия не хватает даже без ограничения по дате (у неликвидных имён
    брокеров физически меньше десяти) — отдаём всё, что есть, с
    coverage_exhausted: true. Пустой ответ вместо данных здесь бесполезен.
    """
    check_auth(authorization)
    hk_symbol = symbol.zfill(5)
    df = call_akshare_with_retry(
        ak.stock_hk_profit_forecast_et, symbol=hk_symbol, indicator=_BROKER_INDICATOR
    )

    base = {
        "source": "etnet",
        "indicator": _BROKER_INDICATOR,
        "symbol": hk_symbol,
        "today": _today(),
        "retrieved_at": _now_iso(),
    }

    if df is None or df.empty:
        return {
            **base,
            "found": False,
            "note": "не найдено — нет покрытия аналитиками либо неверный код",
            "data": [],
        }

    dropped = []
    if not include_past:
        df, dropped = _drop_past_periods(df)

    dates = pd.to_datetime(df[_COL_DATE], errors="coerce")
    now = pd.Timestamp(datetime.now(timezone.utc).date())

    window_used = None
    coverage_exhausted = False
    windows = [max_age_days] + [d for d in _WINDOW_LADDER_DAYS if d > max_age_days]
    for days in windows:
        # Строки с нераспознанной датой оставляем — потерять прогноз хуже, чем
        # показать его без подтверждённой свежести (дата всё равно видна в строке).
        keep = dates.isna() | (dates >= now - pd.Timedelta(days=days))
        if df.loc[keep, _COL_BROKER].nunique() >= min_brokers:
            df = df[keep]
            window_used = days
            break
    else:
        coverage_exhausted = df[_COL_BROKER].nunique() < min_brokers

    df = _mark_outliers(df)

    return {
        **base,
        "found": True,
        "window_used_days": window_used,
        "requested_max_age_days": max_age_days,
        "min_brokers": min_brokers,
        "brokers_count": int(df[_COL_BROKER].nunique()),
        "rows_count": int(len(df)),
        "coverage_exhausted": coverage_exhausted,
        "dropped_past_periods": dropped,
        "data": _df_to_records(df),
    }


# ---------------------------------------------------------------------------
# 4. Агрегатор — собирает применимые источники по одному коду
# ---------------------------------------------------------------------------

def _source_error(exc) -> dict:
    """
    Единый формат ошибки одного источника внутри /forecast/aggregate — сбой
    одного источника не должен обрывать ответ по остальным применимым.
    """
    if isinstance(exc, UpstreamError):
        return {
            "error": True,
            "error_type": exc.error_type,
            "message": exc.message,
            "retryable": exc.retryable,
        }
    return {
        "error": True,
        "error_type": "internal_error",
        "message": str(exc.detail),
        "retryable": False,
    }


@router.get("/aggregate/{symbol}")
def forecast_aggregate(
    symbol: str,
    hk_symbol: Optional[str] = Query(
        None, description="Если у бумаги есть отдельный HK-код, отличный от symbol"
    ),
    include_past: bool = Query(
        False, description="true — вернуть также прогнозы на уже прошедшие годы"
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
    results = {
        "symbol": symbol,
        "today": _today(),
        "retrieved_at": _now_iso(),
        "sources": {},
    }

    is_a_share_like = symbol.isdigit() and len(symbol) == 6

    if is_a_share_like:
        try:
            results["sources"]["eastmoney"] = forecast_em(
                symbol, include_past=include_past, authorization=authorization
            )
        except (UpstreamError, HTTPException) as e:
            results["sources"]["eastmoney"] = _source_error(e)
        try:
            results["sources"]["10jqka"] = forecast_ths(
                symbol,
                indicator=_THS_DEFAULT_INDICATOR,
                include_past=include_past,
                authorization=authorization,
            )
        except (UpstreamError, HTTPException) as e:
            results["sources"]["10jqka"] = _source_error(e)

    hk_code = hk_symbol or (symbol if not is_a_share_like else None)
    if hk_code:
        try:
            results["sources"]["etnet"] = forecast_hk(
                hk_code,
                indicator=_ET_DEFAULT_INDICATOR,
                include_past=include_past,
                authorization=authorization,
            )
        except (UpstreamError, HTTPException) as e:
            results["sources"]["etnet"] = _source_error(e)

    if not results["sources"]:
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить ни один применимый источник для этого symbol. "
            "Укажите hk_symbol явно, если это гонконгская бумага.",
        )

    return results

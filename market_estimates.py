"""
market_estimates.py

Три показателя, которых нет ни в ET Net, ни в AASTOCKS, но которые нужны для
многолетней сводной таблицы: прогноз выручки, прогноз BVPS и будущий курс
CNY/HKD. Каждый добывается своим способом, и у каждого своя честная граница
применимости — она возвращается в ответе, а не прячется.

  1. Выручка  — Yahoo Finance (внутренний quoteSummary/earningsTrend).
                Консенсус S&P Global / TipRanks. Только ДВА горизонта:
                текущий финансовый год (0y) и следующий (+1y). Дальше нет
                ни у кого в бесплатном доступе.

  2. BVPS     — расчёт, а не источник: clean surplus roll-forward
                BVPS(t+1) = BVPS(t) + EPS(t+1) - DPS(t+1).
                EPS/DPS берутся из ET Net (см. forecast_endpoints), стартовый
                BVPS передаёт вызывающий — он у него уже посчитан по правилу 7
                (капитал обыкновенных акционеров / число акций).

  3. Курс     — не прогноз аналитика, а форвард, вменённый рынком: спот CFETS
                плюс своп-пункты. Горизонт — до 1 года, дальше CFETS не котирует.

ВАЖНО про Yahoo. Это внутренний API их сайта, а не публичный продукт: он
требует cookie + crumb, не имеет SLA и формально не предназначен для
сторонних клиентов (на нём же живёт популярная библиотека yfinance). Поэтому:
  - результат кэшируется на тикер (_CACHE_TTL_SECONDS);
  - есть суточный потолок живых обращений (_DAILY_FETCH_BUDGET);
  - любой сбой отдаётся как обычная ошибка апстрима и НЕ роняет остальные
    показатели таблицы — вызывающий должен уметь показать таблицу без выручки.
Если источник однажды закроется — это ожидаемый сценарий, а не авария.
"""

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import akshare as ak
import requests

from errors import UpstreamError, call_akshare_with_retry

logger = logging.getLogger("china_securities_proxy.market_estimates")

REQUEST_TIMEOUT_SECONDS = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

_CACHE_TTL_SECONDS = 6 * 60 * 60
_DAILY_FETCH_BUDGET = 60

_cache: dict = {}
_budget: dict = {"date": None, "count": 0}

# Сессия с cookie и crumb живёт дольше одного запроса, но не вечно; при 401
# добываем заново ровно один раз (см. _yahoo_get).
_session_lock = threading.Lock()
_session: dict = {"session": None, "crumb": None}


def _consume_budget() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if _budget["date"] != today:
        _budget["date"], _budget["count"] = today, 0
    if _budget["count"] >= _DAILY_FETCH_BUDGET:
        raise UpstreamError(
            "rate_limited",
            429,
            f"Суточный лимит обращений к Yahoo Finance исчерпан ({_DAILY_FETCH_BUDGET} тикеров). "
            "Уже запрошенные тикеры продолжают отдаваться из кэша; "
            "новые будут доступны после полуночи UTC.",
            True,
        )
    _budget["count"] += 1


# ---------------------------------------------------------------------------
# 1. Выручка — Yahoo Finance
# ---------------------------------------------------------------------------

def to_yahoo_symbol(symbol: str, market: Optional[str] = None) -> str:
    """
    Приводит код к тому виду, который понимает Yahoo. Проверено вживую:
      HK  — ровно 4 знака с ведущими нулями: "0700.HK", "9988.HK".
            И "700.HK", и "00700.HK" дают 404, поэтому zfill(5) из остальной
            кодовой базы здесь НЕ подходит.
      A   — Шанхай ".SS" (коды 5*/6*/9*), Шэньчжэнь ".SZ" (0*/2*/3*).
    """
    code = re.sub(r"\.(HK|SS|SZ)$", "", symbol.strip().upper())
    if market == "hk" or (market is None and len(code.lstrip("0")) <= 5 and len(code) <= 5):
        return f"{code.lstrip('0').zfill(4)}.HK"
    suffix = "SS" if code[:1] in ("5", "6", "9") else "SZ"
    return f"{code.zfill(6)}.{suffix}"


def _new_yahoo_session():
    session = requests.Session()
    # Первый заход выдаёт cookie согласия, без него crumb не выдаётся.
    session.get("https://fc.yahoo.com", headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    resp = session.get(
        "https://query2.finance.yahoo.com/v1/test/getcrumb",
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    crumb = resp.text.strip()
    if not crumb:
        raise UpstreamError(
            "upstream_response_changed",
            502,
            "Yahoo не выдал crumb — схема авторизации их внутреннего API изменилась.",
            False,
        )
    return session, crumb


# Маркер «тикера нет у источника» — отличается от пустого ответа тем, что
# гарантированно не приведёт к попытке разобрать несуществующий результат.
_NOT_FOUND: dict = {"__not_found__": True}


def _yahoo_get(path: str, params: dict) -> dict:
    def _do_get():
        with _session_lock:
            if _session["session"] is None:
                _session["session"], _session["crumb"] = _new_yahoo_session()
            session, crumb = _session["session"], _session["crumb"]

        url = f"https://query2.finance.yahoo.com{path}"
        resp = session.get(
            url, headers=HEADERS, params={**params, "crumb": crumb},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if resp.status_code == 401:
            # Crumb протух — пересоздаём сессию ровно один раз, без цикла.
            with _session_lock:
                _session["session"], _session["crumb"] = _new_yahoo_session()
                session, crumb = _session["session"], _session["crumb"]
            resp = session.get(
                url, headers=HEADERS, params={**params, "crumb": crumb},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        if resp.status_code == 404:
            # Yahoo не знает такой тикер. Это отсутствие покрытия, а не сбой:
            # ретраить нечего и «попробуйте позже» здесь вводит в заблуждение.
            # Тот же случай, что «No tables found» у ET Net.
            return _NOT_FOUND
        resp.raise_for_status()
        return resp.json()

    return call_akshare_with_retry(_do_get)


def _estimate_block(node: dict, key: str) -> dict:
    block = node.get(key) or {}
    return {
        "avg": (block.get("avg") or {}).get("raw"),
        "low": (block.get("low") or {}).get("raw"),
        "high": (block.get("high") or {}).get("raw"),
        "analysts": (block.get("numberOfAnalysts") or {}).get("raw"),
    }


def fetch_revenue_forecast(symbol: str, market: Optional[str] = None) -> dict:
    """
    Консенсус по выручке (и заодно по EPS) с Yahoo Finance.

    Возвращает только годовые горизонты (0y/+1y) — квартальные для многолетней
    таблицы бесполезны и лишь путают. Каждый горизонт несёт число аналитиков:
    консенсус по двум домам и по сорока — не одно и то же, и вызывающий должен
    иметь возможность это показать.
    """
    yahoo_symbol = to_yahoo_symbol(symbol, market)

    cached = _cache.get(yahoo_symbol)
    now = time.time()
    if cached is not None and (now - cached["ts"]) < _CACHE_TTL_SECONDS:
        return {**cached["data"], "cached": True, "cache_age_seconds": int(now - cached["ts"])}

    _consume_budget()
    payload = _yahoo_get(
        f"/v10/finance/quoteSummary/{yahoo_symbol}",
        {"modules": "earningsTrend,price,financialData"},
    )

    result = None
    if not payload.get("__not_found__"):
        result = ((payload.get("quoteSummary") or {}).get("result") or [None])[0]
    if not result:
        data = {
            "source": "yahoo_finance",
            "symbol": symbol,
            "yahoo_symbol": yahoo_symbol,
            "found": False,
            "note": "Yahoo не знает такого тикера либо не имеет по нему оценок аналитиков",
            "financial_currency": None,
            "trading_currency": None,
            "periods": [],
        }
        _cache[yahoo_symbol] = {"data": data, "ts": now}
        return {**data, "cached": False, "cache_age_seconds": 0}

    # У гонконгских листингов это ДВЕ разные валюты, и их легко перепутать:
    # 9988 торгуется в HKD, а отчитывается и прогнозируется в CNY. Выручка и
    # EPS ниже — в financial_currency, не в trading_currency.
    trading_currency = (result.get("price") or {}).get("currency")
    financial_currency = (result.get("financialData") or {}).get("financialCurrency")
    trend = (result.get("earningsTrend") or {}).get("trend") or []

    periods = []
    for node in trend:
        # 0q/+1q — кварталы, для годовой таблицы не нужны.
        if node.get("period") not in ("0y", "+1y"):
            continue
        revenue = _estimate_block(node, "revenueEstimate")
        earnings = _estimate_block(node, "earningsEstimate")
        if revenue["avg"] is None and earnings["avg"] is None:
            continue
        periods.append(
            {
                "period": node.get("period"),
                "fiscal_year_end": node.get("endDate"),
                "revenue": revenue,
                "eps": earnings,
            }
        )

    data = {
        "source": "yahoo_finance",
        "symbol": symbol,
        "yahoo_symbol": yahoo_symbol,
        "found": bool(periods),
        "financial_currency": financial_currency,
        "trading_currency": trading_currency,
        "currency_note": (
            "Выручка и EPS ниже выражены в financial_currency (валюта отчётности). "
            "trading_currency — валюта торгов на бирже, у гонконгских листингов "
            "китайских эмитентов они РАЗНЫЕ. Не подставляй одно вместо другого."
        ),
        "horizon_note": (
            "Yahoo отдаёт консенсус только на текущий и следующий финансовый год. "
            "Более далёкие годы бесплатно не публикует ни один известный источник — "
            "оставляй их пустыми, а не экстраполируй."
        ),
        "periods": periods,
    }
    if not periods:
        data["note"] = "тикер найден, но оценок аналитиков по выручке/EPS нет"

    _cache[yahoo_symbol] = {"data": data, "ts": now}
    return {**data, "cached": False, "cache_age_seconds": 0}


# ---------------------------------------------------------------------------
# 1a. Автоопределение масштаба EPS у ET Net
# ---------------------------------------------------------------------------
#
# ET Net отдаёт 每股盈利/每股派息 в сотых долях валюты отчётности (аналог
# центов/仙/分), а не в целых единицах: EPS 511.75 у 9988 означает 5.12 CNY.
# Раньше это лечилось инструкцией «сравни порядок величины с историей на глаз»
# (правило 18l). Теперь есть второй независимый источник EPS в заведомо
# правильных единицах — Yahoo, — и множитель можно посчитать, а не угадать.
#
# Сверка на трёх бумагах (ET Net -> Yahoo): 9988 511.75 -> 5.78 (x88.5),
# 3988 73.5 -> 0.752 (x97.7), 1398 103.50 -> 1.036 (x99.9). Разброс — обычная
# разница составов панелей аналитиков, а не разные единицы, поэтому отношение
# округляется до ближайшей степени десяти.

_SCALE_MIN_RATIO = 0.34  # log10 ≈ -0.47: дальше округление до степени десяти неоднозначно
_SCALE_MAX_RATIO = 3000.0


def detect_eps_scale(
    symbol: str,
    etnet_eps_by_year: dict,
    market: Optional[str] = None,
) -> dict:
    """
    Определяет, во сколько раз нужно поделить EPS из ET Net, сверяя его с
    консенсусом Yahoo по тем же финансовым годам.

    Возвращает divisor и доказательства построчно, чтобы решение было
    проверяемым, а не «модель так решила». Если сверить не с чем (Yahoo не
    знает тикер, годы не пересеклись) — divisor не выдумывается, возвращается
    null и confidence "none": лучше отказ, чем подставленный наугад множитель.
    """
    evidence = []
    result = {
        "symbol": symbol,
        "divisor": None,
        "confidence": "none",
        "reference_source": "yahoo_finance",
        "evidence": evidence,
    }

    try:
        reference = fetch_revenue_forecast(symbol, market)
    except UpstreamError as exc:
        result["note"] = f"сверка недоступна: {exc.message}"
        return result

    if not reference.get("found"):
        result["note"] = "у Yahoo нет консенсуса по этому тикеру — сверить масштаб не с чем"
        return result

    yahoo_eps_by_year = {}
    for period in reference["periods"]:
        end = period.get("fiscal_year_end") or ""
        eps = (period.get("eps") or {}).get("avg")
        if eps:
            match = re.match(r"(\d{4})", str(end))
            if match:
                yahoo_eps_by_year[int(match.group(1))] = float(eps)

    ratios = []
    for year, etnet_eps in etnet_eps_by_year.items():
        reference_eps = yahoo_eps_by_year.get(int(year))
        if reference_eps is None or not reference_eps or etnet_eps is None:
            continue
        # Знаки должны совпадать: прибыль против убытка — это расхождение по
        # существу, а не по единицам, и «чинить» его множителем нельзя.
        if (float(etnet_eps) > 0) != (reference_eps > 0):
            continue
        ratio = abs(float(etnet_eps)) / abs(reference_eps)
        if _SCALE_MIN_RATIO <= ratio <= _SCALE_MAX_RATIO:
            ratios.append(ratio)
            evidence.append(
                {
                    "fiscal_year": int(year),
                    "etnet_eps": float(etnet_eps),
                    "yahoo_eps": reference_eps,
                    "ratio": round(ratio, 2),
                }
            )

    if not ratios:
        result["note"] = "финансовые годы ET Net и Yahoo не пересеклись — сверить нечего"
        return result

    ratios.sort()
    median = ratios[len(ratios) // 2]
    exponent = round(_log10(median))
    divisor = float(10 ** exponent)

    # Насколько медиана отклонилась от той степени десяти, к которой её свели.
    drift = abs(median - divisor) / divisor
    if drift <= 0.25:
        confidence = "high" if len(ratios) >= 2 else "medium"
    elif drift <= 0.5:
        confidence = "medium"
    else:
        confidence = "low"

    result.update(
        {
            "divisor": divisor,
            "confidence": confidence,
            "median_ratio": round(median, 2),
            "drift_from_power_of_ten": round(drift, 3),
            "note": (
                f"EPS/DPS из ET Net делить на {divisor:g}. Множитель получен сверкой с "
                f"консенсусом Yahoo по {len(ratios)} совпавшим финансовым годам, а не "
                "подобран на глаз. Если confidence=low — покажи пользователю оба числа "
                "и не подставляй значение молча."
            ),
        }
    )
    return result


def _log10(value: float) -> float:
    import math

    return math.log10(value)


# ---------------------------------------------------------------------------
# 2. BVPS — расчёт roll-forward, а не источник
# ---------------------------------------------------------------------------

# Порог, после которого расчётный BVPS перестаёт быть осмысленным: за пять лет
# накопленная ошибка от байбеков/допэмиссии съедает всякую точность.
_MAX_ROLL_FORWARD_YEARS = 5


def roll_forward_bvps(
    last_bvps: float,
    last_bvps_year: int,
    eps_by_year: dict,
    dps_by_year: dict,
) -> dict:
    """
    Clean surplus roll-forward: BVPS(t+1) = BVPS(t) + EPS(t+1) - DPS(t+1).

    Это не прогноз аналитика, а арифметика поверх чужих прогнозов EPS/DPS, и
    у неё есть ровно одно допущение: весь прибыток и убыток проходит через
    капитал, а других движений капитала нет. Что она НЕ учитывает:
      - выкуп акций (уменьшает капитал; у эмитентов вроде Alibaba это крупная
        статья, и расчёт систематически завышает BVPS);
      - допэмиссию и конвертацию (наоборот, занижает);
      - прочий совокупный доход, в т.ч. валютную переоценку;
      - изменение числа акций — знаменатель считается постоянным.
    Для банков с ровной дивидендной политикой ошибка мала, для растущих
    техов с большими байбеками — заметна. Поэтому каждый год возвращается с
    накопленным числом шагов: чем дальше, тем меньше веры.
    """
    steps = []
    bvps = float(last_bvps)
    year = int(last_bvps_year)

    for offset in range(1, _MAX_ROLL_FORWARD_YEARS + 1):
        target = year + offset
        eps = eps_by_year.get(target)
        dps = dps_by_year.get(target)
        if eps is None:
            # Без прогноза прибыли катить дальше нечего — обрываем цепочку,
            # а не подставляем ноль: ноль здесь означал бы "прибыли не будет".
            break
        retained = float(eps) - float(dps or 0.0)
        bvps = bvps + retained
        steps.append(
            {
                "year": target,
                "bvps": round(bvps, 4),
                "eps_used": float(eps),
                "dps_used": float(dps) if dps is not None else None,
                "dps_assumed_zero": dps is None,
                "steps_from_actual": offset,
                "confidence": "high" if offset == 1 else ("medium" if offset == 2 else "low"),
            }
        )

    result = {
        "method": "clean_surplus_roll_forward",
        "formula": "BVPS(t+1) = BVPS(t) + EPS(t+1) - DPS(t+1)",
        "base_bvps": float(last_bvps),
        "base_year": year,
        "caveat": (
            "Расчёт, а не прогноз аналитика. Не учитывает выкуп акций, допэмиссию, "
            "прочий совокупный доход и изменение числа акций. У эмитентов с крупными "
            "байбеками (например Alibaba) систематически завышает BVPS — указывай это "
            "в сноске рядом со значением."
        ),
        "years": steps,
    }

    # Пустой результат почти всегда означает не «нет прогнозов», а разрыв:
    # база передана за слишком старый год, и первого шага (base_year + 1) в
    # прогнозах уже нет. Молча отдавать пустой список здесь вредно - вызывающий
    # решит, что покрытия нет вовсе, хотя достаточно сдвинуть базу на свежий
    # завершённый финансовый год.
    if not steps and eps_by_year:
        available = sorted(int(y) for y in eps_by_year)
        future = [y for y in available if y > year]
        if future:
            result["note"] = (
                f"Прогноз EPS на {year + 1} отсутствует, поэтому цепочку не от чего "
                f"начать: у источника есть только {future}. Передай last_bvps за "
                f"последний ЗАВЕРШЁННЫЙ финансовый год, ближайший к {future[0] - 1}, "
                "иначе между базой и первым прогнозом останется год без данных."
            )
        else:
            result["note"] = (
                f"Все прогнозные годы источника ({available}) не позже базового "
                f"{year} - катить BVPS вперёд не от чего."
            )
        result["available_forecast_years"] = available

    return result


# ---------------------------------------------------------------------------
# 3. Курс — форвард, вменённый рынком (CFETS спот + своп-пункты)
# ---------------------------------------------------------------------------

# Своп-пункты CFETS котируются в пипсах. Для пар против CNY пипс = 1e-4.
# Проверено перекрёстно: HKD привязан к USD (диапазон 7.75-7.85), поэтому
# форвард HKD/CNY можно получить двумя независимыми путями — напрямую и через
# USD/CNY, поделённый на курс USD/HKD. При пипсе 1e-4 они сходятся (0.8443 vs
# 0.8369, расхождение объясняется форвардными пунктами самого USD/HKD, которые
# в кросс-проверке приняты нулевыми); при 1e-5 расходятся втрое сильнее.
# Множитель зашит не «по документации», а подтверждён этой сверкой — и та же
# сверка выполняется в рантайме, см. _cross_check_via_usd.
_SWAP_PIP = 1e-4

_TENORS = {"1周": 7, "1月": 30, "3月": 91, "6月": 182, "9月": 273, "1年": 365}

# Если прямой и кросс-путь разойдутся сильнее этого — молча цифру не отдаём.
_CROSS_CHECK_TOLERANCE = 0.03


def _mid(raw) -> Optional[float]:
    """Котировки CFETS приходят как 'bid/ask' одной строкой."""
    try:
        parts = str(raw).split("/")
        values = [float(p) for p in parts if p not in ("", "nan")]
        return sum(values) / len(values) if values else None
    except (TypeError, ValueError):
        return None


def _pair_row(df, pair: str):
    rows = df[df["货币对"] == pair]
    return rows.iloc[0] if not rows.empty else None


def fx_forward_cny_hkd() -> dict:
    """
    Форвардный курс CNY/HKD, вменённый рынком: спот CFETS плюс своп-пункты.

    Это не мнение аналитика о будущем курсе, а цена, по которой рынок прямо
    сейчас готов обменять валюту в будущем — для таблицы это честнее прогноза.
    Горизонт ограничен годом: дальше CFETS не котирует, и никакого бесплатного
    источника многолетнего прогноза курса не существует.
    """
    spot_df = call_akshare_with_retry(ak.fx_spot_quote)
    swap_df = call_akshare_with_retry(ak.fx_swap_quote)

    spot_row = _pair_row(spot_df, "HKD/CNY")
    swap_row = _pair_row(swap_df, "HKD/CNY")
    if spot_row is None or swap_row is None:
        raise UpstreamError(
            "upstream_response_changed",
            502,
            "CFETS больше не отдаёт пару HKD/CNY — структура ответа изменилась.",
            False,
        )

    spot_hkd_cny = (float(spot_row["买报价"]) + float(spot_row["卖报价"])) / 2

    tenors = []
    for label, days in _TENORS.items():
        points = _mid(swap_row.get(label))
        if points is None:
            continue
        forward_hkd_cny = spot_hkd_cny + points * _SWAP_PIP
        if forward_hkd_cny <= 0:
            continue
        tenors.append(
            {
                "tenor": label,
                "approx_days": days,
                "swap_points_raw": points,
                "hkd_cny": round(forward_hkd_cny, 6),
                "cny_hkd": round(1 / forward_hkd_cny, 4),
            }
        )

    return {
        "source": "cfets",
        "pair": "CNY/HKD",
        "basis": "market_implied_forward",
        "spot_hkd_cny": round(spot_hkd_cny, 6),
        "spot_cny_hkd": round(1 / spot_hkd_cny, 4),
        "swap_pip": _SWAP_PIP,
        "max_horizon": "1 год",
        "horizon_note": (
            "CFETS котирует своп-пункты максимум на год. Курс на более далёкие годы "
            "оставляй пустым: многолетнего прогноза курса нет ни в одном бесплатном "
            "источнике, а линейная экстраполяция здесь бессмысленна."
        ),
        "cross_check": _cross_check_via_usd(spot_df, swap_df, spot_hkd_cny),
        "tenors": tenors,
    }


def _cross_check_via_usd(spot_df, swap_df, spot_hkd_cny: float) -> dict:
    """
    Независимая проверка годового форварда вторым путём: HKD привязан к USD,
    значит форвард HKD/CNY можно вывести из форварда USD/CNY, поделив на курс
    USD/HKD. Если два пути разойдутся — значит либо сломалась конвенция
    своп-пунктов, либо изменилась структура ответа, и цифру нельзя отдавать
    молча (ровно тот случай, ради которого появилось правило 18l про единицы).
    """
    result = {"available": False}
    usd_spot_row = _pair_row(spot_df, "USD/CNY")
    usd_swap_row = _pair_row(swap_df, "USD/CNY")
    if usd_spot_row is None or usd_swap_row is None:
        return result

    hkd_swap_row = _pair_row(swap_df, "HKD/CNY")
    usd_points = _mid(usd_swap_row.get("1年"))
    hkd_points = _mid(hkd_swap_row.get("1年")) if hkd_swap_row is not None else None
    if usd_points is None or hkd_points is None:
        return result

    spot_usd_cny = (float(usd_spot_row["买报价"]) + float(usd_spot_row["卖报价"])) / 2
    usd_hkd = spot_usd_cny / spot_hkd_cny  # должен попадать в коридор пега 7.75-7.85

    direct = spot_hkd_cny + hkd_points * _SWAP_PIP
    via_usd = (spot_usd_cny + usd_points * _SWAP_PIP) / usd_hkd
    deviation = abs(direct - via_usd) / direct if direct else None

    agrees = deviation is not None and deviation <= _CROSS_CHECK_TOLERANCE
    result = {
        "available": True,
        "implied_usd_hkd_spot": round(usd_hkd, 4),
        "peg_band_ok": 7.70 <= usd_hkd <= 7.90,
        "forward_direct": round(direct, 6),
        "forward_via_usd_peg": round(via_usd, 6),
        "deviation": round(deviation, 4) if deviation is not None else None,
        "agrees": agrees,
    }
    if not agrees:
        result["warning"] = (
            "Прямой и кросс-путь разошлись сильнее допустимого. Возможно изменилась "
            "конвенция своп-пунктов у источника. Не используй форвард, пока не проверишь."
        )
    return result

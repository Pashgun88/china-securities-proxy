"""
broker_consensus.py

Разовый парсер консенсус-прогнозов брокеров с AASTOCKS.com для HK-тикеров.

ВАЖНО — интерактивный, но троттлящийся источник, не для автоматического опроса.
AASTOCKS не публикует официального API для этих данных; страницы парсятся
через BeautifulSoup, поэтому нагрузка на сайт ограничивается осознанно:

  - модуль НЕ подключён к keep-warm workflow и не вызывается по расписанию
    ни одной частью системы — только в ответ на живой запрос пользователя;
  - кэш на тикер (_CACHE_TTL_SECONDS): повторные вопросы про одну и ту же
    бумагу до сайта не доходят вообще;
  - суточный потолок живых обходов (_DAILY_FETCH_BUDGET), при исчерпании —
    429, а не тихое продолжение опроса;
  - пауза REQUEST_DELAY_SECONDS между статьями внутри одного обхода.

Изначально эндпоинт был помечен manual-only (1-2 запуска в год); после
подключения к GPT частота определяется вопросами пользователя, и именно
поэтому появились кэш и потолок. Если и этого перестанет хватать — вопрос
нужно решать не поднятием лимита, а лицензированием данных или переходом
на официальный источник.

Формат страниц AASTOCKS не документирован и может измениться в любой момент —
парсинг это учитывает: сбои извлечения (TypeError/KeyError/IndexError/
AttributeError) классифицируются как upstream_response_changed и не
ретраятся (см. errors.py), а не путаются с реальными сетевыми сбоями.

Из-за разнородности брокерских отчётов (сводная таблица по нескольким
эмитентам vs. текст-заметка по одному эмитенту) извлечение rating/target
price эвристическое — в каждой записи всегда возвращается raw_snippet
и headline, чтобы можно было проверить/поправить результат вручную.
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

from errors import UpstreamError, call_akshare_with_retry

logger = logging.getLogger("china_securities_proxy.aastocks")

BASE_URL = "https://www.aastocks.com"
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_DELAY_SECONDS = 1.5  # между запросами, если тикеров несколько за вызов

# Обычный браузерный User-Agent, без маскировки под что-либо специфическое.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

_PARSING_ERRORS = (TypeError, KeyError, IndexError, AttributeError)

# Троттлинг. Эндпоинт вызывается из GPT, то есть частота определяется тем,
# сколько вопросов задаст пользователь, а не расписанием. Чтобы это не
# превращалось в поток обращений к AASTOCKS, режем в два слоя: кэш на тикер
# (повторные вопросы про одну бумагу вообще не доходят до сайта) и жёсткий
# суточный потолок на число живых обходов. Один обход — это листинг плюс по
# запросу на каждую заметку, поэтому потолок считается в обходах, а не в
# HTTP-запросах.
_CACHE_TTL_SECONDS = 6 * 60 * 60
_DAILY_FETCH_BUDGET = 20

_cache: dict = {}
_budget: dict = {"date": None, "count": 0}


def _consume_budget() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if _budget["date"] != today:
        _budget["date"], _budget["count"] = today, 0
    if _budget["count"] >= _DAILY_FETCH_BUDGET:
        raise UpstreamError(
            "rate_limited",
            429,
            f"Суточный лимит обращений к AASTOCKS исчерпан ({_DAILY_FETCH_BUDGET} тикеров). "
            "Данные по уже запрошенным тикерам продолжают отдаваться из кэша; "
            "новые тикеры будут доступны после полуночи UTC.",
            True,
        )
    _budget["count"] += 1


def _normalize_hk_symbol(symbol: str) -> str:
    code = symbol.strip().upper()
    code = re.sub(r"\.HK$", "", code)
    return code.zfill(5)


def _http_get(url: str) -> str:
    def _do_get():
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.text

    return call_akshare_with_retry(_do_get)


# Заголовок research-заметки AASTOCKS почти всегда несёт всё нужное сам:
#   "Daiwa Slightly Trims XIAOMI-W TP to HKD32, Reiterates Buy"
#   "BofAS Ratings, TPs on H-Share CN Banks (Table)"
#   "G Sachs: Large CN Banks Remain Top Picks"
# То есть имя дома стоит первым и отделено от остального либо двоеточием,
# либо глаголом действия. Разбирать структурно надёжнее, чем брать два первых
# слова: у "Daiwa Slightly Trims..." так получалось имя брокера "Daiwa Slightly".
_ACTION_WORDS = {
    "cuts", "trims", "slashes", "reduces", "lowers", "raises", "lifts", "hikes",
    "boosts", "upgrades", "downgrades", "reiterates", "maintains", "keeps",
    "initiates", "resumes", "starts", "says", "forecasts", "expects", "sees",
    "foresees", "prefers", "favors", "favours", "recommends", "believes",
    "estimates", "projects", "ratings", "points", "remains", "turns",
}

_RATING_WORDS = [
    "Outperform", "Underperform", "Overweight", "Underweight", "Accumulate",
    "Neutral", "Buy", "Sell", "Hold", "Reduce", "Add",
]
_RATING_RE = re.compile(r"\b(%s)\b" % "|".join(_RATING_WORDS), re.I)

_CURRENCY = r"(?:HKD|HK\$|RMB|CNY|USD|US\$)?"
_TP_FROM_TO_RE = re.compile(
    r"TPs?\s+from\s*%s\s*(\d+(?:\.\d+)?)\s*(?:to|->)\s*%s\s*(\d+(?:\.\d+)?)" % (_CURRENCY, _CURRENCY),
    re.I,
)
_TP_TO_RE = re.compile(r"TPs?\s+(?:to|at)\s*%s\s*(\d+(?:\.\d+)?)" % _CURRENCY, re.I)
_TP_CURRENCY_RE = re.compile(r"\b(HKD|HK\$|RMB|CNY|USD|US\$)\s*\d", re.I)


def _clean_broker(name: str) -> str:
    # "HSBC Research's Top Picks..." — притяжательное окончание в имя дома не входит.
    return re.sub(r"['’]s$", "", name.strip(" ,:"))


def _guess_broker(headline: str) -> str:
    """
    Имя брокера — всё, что стоит до двоеточия либо до первого глагола действия.
    Хвостовые наречия ("Slightly") отбрасываем: они относятся к действию, а не
    к названию дома. Точность не гарантируется — сверяйте с полем headline.
    """
    head = headline.strip()
    if ":" in head:
        candidate = head.split(":", 1)[0].strip()
        if 0 < len(candidate) <= 30:
            return _clean_broker(candidate)

    words = head.split()
    collected = []
    for word in words:
        if re.sub(r"[^A-Za-z]", "", word).lower() in _ACTION_WORDS:
            break
        collected.append(word)
        # Страховка от заголовков без распознанного глагола: имя дома — это
        # одно-два слова ("G Sachs", "BofAS"), а не половина предложения.
        if len(collected) >= 4:
            return _clean_broker(" ".join(words[:2]))

    while collected and collected[-1].lower().endswith("ly"):
        collected.pop()

    if collected:
        return _clean_broker(" ".join(collected))
    return _clean_broker(" ".join(words[:2])) if words else headline


def _extract_from_headline(headline: str) -> dict:
    """
    Достаёт рейтинг и целевую цену прямо из заголовка. Это чаще срабатывает,
    чем разбор текста статьи: в заголовке данные стоят в фиксированной форме,
    а в теле встречаются в свободном пересказе.
    """
    tp_old = tp_new = None
    match = _TP_FROM_TO_RE.search(headline)
    if match:
        tp_old, tp_new = float(match.group(1)), float(match.group(2))
    else:
        match = _TP_TO_RE.search(headline)
        if match:
            tp_new = float(match.group(1))

    currency = None
    currency_match = _TP_CURRENCY_RE.search(headline)
    if tp_new is not None and currency_match:
        currency = currency_match.group(1).upper().replace("$", "D")

    ratings = _RATING_RE.findall(headline)

    return {
        "rating": ratings[-1] if ratings else None,
        "target_price_old": tp_old,
        "target_price_new": tp_new,
        "target_price_currency": currency,
    }


def _extract_table_row(text: str, hk_symbol: str):
    """Извлекает rating/TP из строк вида '(00939.HK) ... | Outperform | 12.3 -> 12.5'."""
    marker = f"({hk_symbol}.HK)"
    idx = text.find(marker)
    if idx == -1:
        return None
    window = text[idx : idx + 600]
    m = re.search(r"\|\s*([^|\n]+?)\s*\|\s*([^|\n]+)", window)
    if not m:
        return None
    rating_raw, price_raw = m.group(1).strip(), m.group(2).strip()
    prices = re.findall(r"\d+(?:\.\d+)?", price_raw)
    if not prices:
        # Похоже на заголовок таблицы ("Stock | Investment Rating | TP"),
        # а не на строку с данными конкретного тикера — например, если сам
        # тикер упомянут только в прозе выше, а ближайшая по тексту "|...|"
        # строка — это шапка ДРУГОЙ таблицы для других эмитентов.
        return None
    rating = rating_raw.split("->")[-1].strip()
    tp_new = float(prices[-1])
    tp_old = float(prices[0]) if len(prices) > 1 else None
    return {
        "rating": rating,
        "target_price_old": tp_old,
        "target_price_new": tp_new,
        "raw_snippet": window[:300],
    }


_RATING_STOPWORDS = {"well", "such", "follows", "a", "an", "the", "part", "good", "far", "shown"}


def _guess_rating(window: str) -> Optional[str]:
    """
    "X rating remains Buy" — прямое совпадение.
    "rated ... as well as ... as Buy" — идиома "as well as" ломает наивный
    поиск первого "as <слово>" после "rated", поэтому берём последнее
    вхождение "as <слово>", отбросив служебные слова вроде "well"/"the".
    """
    m = re.search(r"rating\s+(?:remains|maintained at)\s+([A-Za-z\-]+)", window, re.I)
    if m:
        return m.group(1)

    rated_match = re.search(r"\brated\b", window, re.I)
    if not rated_match:
        return None
    as_matches = re.findall(r"\bas\s+([A-Za-z\-]+)\b", window[rated_match.end() :], re.I)
    candidates = [w for w in as_matches if w.lower() not in _RATING_STOPWORDS]
    return candidates[-1] if candidates else None


def _extract_prose(text: str, hk_symbol: str):
    """
    Fallback для отчётов-заметок по одному эмитенту (без табличного
    форматирования). Ищет rating/TP только в окне вокруг первого упоминания
    тикера — если искать по всей статье, есть шанс зацепить рейтинг/TP
    другого упомянутого эмитента (сектора-обзоры часто говорят о нескольких
    компаниях сразу).
    """
    marker = f"({hk_symbol}.HK)"
    idx = text.find(marker)
    if idx == -1:
        window = text[:900]
        snippet_start, snippet_end = 0, 400
    else:
        window = text[max(0, idx - 100) : idx + 900]
        snippet_start, snippet_end = max(0, idx - 100), idx + 400

    tp_matches = re.findall(
        r"target price[^.\n]*?(?:from\s+)?(?:HKD|HK\$)?\s*(\d+(?:\.\d+)?)\s*"
        r"(?:to|->)\s*(?:HKD|HK\$)?\s*(\d+(?:\.\d+)?)",
        window,
        re.I,
    )
    tp_old = tp_new = None
    if tp_matches:
        tp_old, tp_new = (float(x) for x in tp_matches[-1])

    rating = _guess_rating(window)

    return {
        "rating": rating,
        "target_price_old": tp_old,
        "target_price_new": tp_new,
        "raw_snippet": text[snippet_start:snippet_end],
    }


def _parse_article(html: str, hk_symbol: str, headline: str, source_url: str, date: Optional[str]) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")
    content_el = soup.find(id="spanContent")
    if content_el is None:
        raise UpstreamError(
            "upstream_response_changed",
            502,
            "AASTOCKS article page: #spanContent not found — page layout may have changed",
            False,
        )
    text = content_el.get_text("\n", strip=True)
    # Стандартный дисклеймер-футер ("...delayed for at least 15 mins.Short
    # Selling Data as at 2026-08-07 12:25.") встречается в каждой статье и
    # содержит буквальное "as at" — без чистки это ложно матчится regex'ами
    # rating/TP как часть содержательного текста.
    text = re.sub(r"\([^()]*(?:quote is delayed|Short Selling Data)[^()]*\)", "", text, flags=re.I)

    extracted = _extract_table_row(text, hk_symbol) or _extract_prose(text, hk_symbol) or {}
    from_headline = _extract_from_headline(headline)

    # Тело статьи приоритетнее (там разбор по конкретному тикеру), но пустые
    # поля добираем из заголовка — иначе заметка вида "JPM Cuts TP to HKD31"
    # возвращалась с rating/TP = null при том, что данные видны в заголовке.
    merged = {
        key: extracted.get(key) if extracted.get(key) is not None else from_headline.get(key)
        for key in ("rating", "target_price_old", "target_price_new")
    }
    merged["target_price_currency"] = from_headline.get("target_price_currency")

    if all(value is None for value in merged.values()):
        return None

    return {
        "broker": _guess_broker(headline),
        "headline": headline,
        **merged,
        "date": date,
        "source_url": source_url,
        "raw_snippet": extracted.get("raw_snippet", text[:400]),
    }


def _list_research_articles(hk_symbol: str) -> list:
    listing_url = f"{BASE_URL}/en/stocks/analysis/stock-aafn/{hk_symbol}/0/hk-stock-news/1"
    html = _http_get(listing_url)

    try:
        soup = BeautifulSoup(html, "html.parser")
        heads = soup.find_all("div", class_="newshead4")
        articles = []
        for head in heads:
            link = head.find("a")
            if link is None:
                continue
            headline = head.get_text(strip=True)
            if not headline.startswith("<Research>"):
                continue
            href = link.get("href")
            if not href:
                continue

            date = None
            time_block = head.find_next_sibling("div", class_="newstime4")
            if time_block is not None:
                vote_total = time_block.find("div", class_="div_VoteTotal")
                raw_dt = vote_total.get("data-nt") if vote_total is not None else None
                if raw_dt and len(raw_dt) == 12:
                    date = f"{raw_dt[0:4]}-{raw_dt[4:6]}-{raw_dt[6:8]} {raw_dt[8:10]}:{raw_dt[10:12]}"

            articles.append(
                {
                    "headline": headline[len("<Research>") :].strip(),
                    "url": BASE_URL + href,
                    "date": date,
                }
            )
        return articles
    except _PARSING_ERRORS as exc:
        logger.warning(
            "aastocks listing parsing failed for %s: %s | page snippet=%r",
            listing_url,
            exc,
            html[:500],
        )
        raise UpstreamError(
            "upstream_response_changed",
            502,
            f"AASTOCKS listing page structure changed: {exc}",
            False,
        ) from exc


def fetch_aastocks_consensus(symbol: str) -> dict:
    """
    Собирает консенсус-прогнозы брокеров по HK-тикеру с новостной ленты
    AASTOCKS ("<Research>..." заметки на странице stock-aafn).

    Обращения к сайту ограничены кэшем и суточным потолком — см. блок
    троттлинга в начале модуля.
    """
    hk_symbol = _normalize_hk_symbol(symbol)

    now = time.time()
    cached = _cache.get(hk_symbol)
    if cached is not None and (now - cached["ts"]) < _CACHE_TTL_SECONDS:
        return {
            **cached["payload"],
            "cached": True,
            "cache_age_seconds": int(now - cached["ts"]),
        }

    _consume_budget()
    articles = _list_research_articles(hk_symbol)

    reports = []
    for i, article in enumerate(articles):
        if i > 0:
            time.sleep(REQUEST_DELAY_SECONDS)

        html = _http_get(article["url"])
        try:
            report = _parse_article(html, hk_symbol, article["headline"], article["url"], article["date"])
        except _PARSING_ERRORS as exc:
            logger.warning(
                "aastocks article parsing failed for %s: %s | page snippet=%r",
                article["url"],
                exc,
                html[:500],
            )
            raise UpstreamError(
                "upstream_response_changed",
                502,
                f"AASTOCKS article page structure changed: {exc}",
                False,
            ) from exc

        if report is not None:
            reports.append(report)

    payload = {
        "source": "aastocks",
        "symbol": hk_symbol,
        "today": datetime.now(timezone.utc).date().isoformat(),
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "found": bool(reports),
        "note": None if reports else "не найдено — нет research-заметок с рейтингом/TP на текущей странице новостей",
        "data": reports,
    }
    _cache[hk_symbol] = {"payload": payload, "ts": now}
    return {**payload, "cached": False, "cache_age_seconds": 0}

"""
broker_consensus.py

Разовый парсер консенсус-прогнозов брокеров с AASTOCKS.com для HK-тикеров.

ВАЖНО — Manual/low-frequency use only, not for automated polling.
Этот модуль/эндпоинт НЕ подключён к keep-warm workflow и не вызывается
автоматически другими частями системы — только по прямому ручному запросу
(ожидается 1-2 запуска в год на несколько тикеров). AASTOCKS не публикует
официального API для этих данных; страницы парсятся через BeautifulSoup.
Соблюдаем ToS сайта тем, что ограничиваемся редкими ручными запросами, а не
постоянным опросом. Если частота использования вырастет — это нужно
пересмотреть отдельно (лицензировать данные или найти официальный источник).

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


def _guess_broker(headline: str) -> str:
    """
    Эвристика: заголовки часто вида "<Broker>: <suть>" или "<Broker> <Verb> ...".
    Не гарантирует точность на 100% — сверяйте с полем headline вручную.
    """
    if ":" in headline:
        candidate = headline.split(":", 1)[0].strip()
        if 0 < len(candidate) <= 30:
            return candidate
    words = headline.split()
    return " ".join(words[:2]) if words else headline


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

    extracted = _extract_table_row(text, hk_symbol) or _extract_prose(text, hk_symbol)
    if extracted is None:
        return None

    return {
        "broker": _guess_broker(headline),
        "headline": headline,
        "rating": extracted["rating"],
        "target_price_old": extracted["target_price_old"],
        "target_price_new": extracted["target_price_new"],
        "date": date,
        "source_url": source_url,
        "raw_snippet": extracted["raw_snippet"],
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
    AASTOCKS ("<Research>..." заметки на странице stock-aafn). Ручной,
    низкочастотный инструмент — см. предупреждение в начале модуля.
    """
    hk_symbol = _normalize_hk_symbol(symbol)
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

    return {
        "source": "aastocks",
        "symbol": hk_symbol,
        "found": bool(reports),
        "note": None if reports else "не найдено — нет research-заметок с рейтингом/TP на текущей странице новостей",
        "data": reports,
    }

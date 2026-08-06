"""
errors.py

Единая классификация сбоев при обращении к внешним источникам через akshare
и обёртка с одним ретраем. akshare под капотом использует requests — сюда
попадают её типовые исключения.

Два принципиально разных случая, которые раньше были неотличимы:

  - Сетевой сбой (нет соединения, таймаут, DNS, 5xx от апстрима) — временное
    явление, имеет смысл ретраить и на клиенте, error_type=upstream_timeout /
    upstream_unavailable, retryable=True.

  - Ошибка разбора уже полученного ответа (TypeError/KeyError/IndexError/
    AttributeError внутри akshare при парсинге JSON) — это НЕ временный сбой
    сети. Обычно означает, что либо источник поменял структуру ответа, либо
    (гораздо чаще на практике) сама akshare-функция получила от источника
    "пустой"/неожиданный JSON из-за неверно сформированного запроса с нашей
    стороны (см. пример: /hk_financial для символа "0175" — Eastmoney не
    находит тикер без ведущих нулей и возвращает result: null, а akshare
    падает при индексации в None). Ретраить бессмысленно — результат
    повторится. error_type=upstream_response_changed, retryable=False.
    Сырой ответ источника (если удалось перехватить) логируется, чтобы
    можно было потом посмотреть, что пришло на самом деле.
"""

import logging
import threading
import time
from typing import Callable, TypeVar

import requests

logger = logging.getLogger("china_securities_proxy.upstream")

RETRY_ATTEMPTS = 1
RETRY_DELAY_SECONDS = 2

T = TypeVar("T")

_PARSING_ERRORS = (TypeError, KeyError, IndexError, AttributeError)

# Перехватываем сырой ответ последнего HTTP-запроса в этом потоке, чтобы было
# что залогировать при ошибке парсинга — сама akshare-функция ответ наружу
# не отдаёт, а поднимает исключение уже после неудачной индексации в JSON.
_last_response = threading.local()
_original_send = requests.Session.send


def _capturing_send(self, request, **kwargs):
    response = _original_send(self, request, **kwargs)
    try:
        _last_response.url = response.url
        _last_response.status_code = response.status_code
        _last_response.text = response.text[:500]
    except Exception:
        pass
    return response


requests.Session.send = _capturing_send


class UpstreamError(Exception):
    """Уже классифицированный сбой апстрима — ловится глобальным хендлером в main.py."""

    def __init__(self, error_type: str, status_code: int, message: str, retryable: bool):
        self.error_type = error_type
        self.status_code = status_code
        self.message = message
        self.retryable = retryable
        super().__init__(message)


def classify_exception(exc: Exception):
    """Возвращает (error_type, http_status, retryable) по типу исключения."""
    if isinstance(exc, (requests.exceptions.Timeout, TimeoutError)):
        return "upstream_timeout", 504, True
    if isinstance(exc, (requests.exceptions.ConnectionError, ConnectionError)):
        return "upstream_unavailable", 502, True
    if isinstance(exc, requests.exceptions.RequestException):
        # HTTPError (в т.ч. 5xx от источника), сетевые протокольные ошибки и т.п.
        return "upstream_unavailable", 502, True
    if isinstance(exc, _PARSING_ERRORS):
        _log_response_snapshot(exc)
        return "upstream_response_changed", 502, False
    return "internal_error", 500, False


def _log_response_snapshot(exc: Exception) -> None:
    url = getattr(_last_response, "url", None)
    status_code = getattr(_last_response, "status_code", None)
    text = getattr(_last_response, "text", None)
    logger.warning(
        "upstream_response_changed: %s: %s | last response url=%s status=%s body[:500]=%r",
        type(exc).__name__,
        exc,
        url,
        status_code,
        text,
    )


def call_akshare_with_retry(func: Callable[..., T], **kwargs) -> T:
    """
    Вызывает akshare-функцию с одним повтором через RETRY_DELAY_SECONDS —
    но только если сбой похож на сетевой. Ошибки парсинга (см. модульный
    docstring) не ретраятся: повтор того же запроса даст тот же результат.
    Пустой DataFrame (данных реально нет) — это не исключение и ретрая не
    касается.
    """
    last_exc: Exception
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            return func(**kwargs)
        except _PARSING_ERRORS as exc:
            last_exc = exc
            break
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)

    error_type, status_code, retryable = classify_exception(last_exc)
    raise UpstreamError(error_type, status_code, str(last_exc)[:300], retryable) from last_exc

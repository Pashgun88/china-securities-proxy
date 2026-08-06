"""
errors.py

Единая классификация сбоев при обращении к внешним источникам через akshare
и обёртка с одним ретраем. akshare под капотом использует requests — сюда
попадают её типовые исключения (Timeout, ConnectionError, HTTPError,
JSONDecodeError), плюс TypeError/KeyError/IndexError/AttributeError —
типичные последствия того, что источник вернул пустой/неожиданный ответ,
а сам akshare парсит его без проверок (см. пример: /hk_financial для 0175 —
'NoneType' object is not subscriptable, потому что Eastmoney вернул null).
"""

import time
from typing import Callable, TypeVar

import requests

RETRY_ATTEMPTS = 1
RETRY_DELAY_SECONDS = 2

T = TypeVar("T")


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
    if isinstance(
        exc,
        (
            requests.exceptions.RequestException,
            TypeError,
            KeyError,
            IndexError,
            AttributeError,
        ),
    ):
        return "upstream_unavailable", 502, True
    return "internal_error", 500, False


def call_akshare_with_retry(func: Callable[..., T], **kwargs) -> T:
    """
    Вызывает akshare-функцию с одним повтором через RETRY_DELAY_SECONDS при
    сбое сети/источника. Пустой DataFrame (данных реально нет) — это не
    исключение и ретрай не затрагивает такие случаи.
    """
    last_exc: Exception
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            return func(**kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)

    error_type, status_code, retryable = classify_exception(last_exc)
    raise UpstreamError(error_type, status_code, str(last_exc)[:300], retryable) from last_exc

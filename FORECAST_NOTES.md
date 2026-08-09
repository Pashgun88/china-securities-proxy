# China Securities Data Proxy — прогнозы консенсуса аналитиков

## Проект

FastAPI-обёртка над [AKShare](https://akshare.akfamily.xyz/) для доступа к
данным китайских (SSE/SZSE) и гонконгских (HKEX) тикеров: отчётность,
дивиденды, котировки, валютные курсы. Задеплоена на Render
(`https://china-securities-proxy.onrender.com`, автодеплой из ветки `main`).
Используется как Action в кастомном GPT (ChatGPT). Bearer-токен
(`PROXY_ACCESS_KEY`) задан в переменных окружения Render, схема Action —
[openapi_schema.yaml](openapi_schema.yaml), системный промпт GPT —
[gpt_instructions.txt](gpt_instructions.txt).

## Что добавлено

Роутер [forecast_endpoints.py](forecast_endpoints.py), подключён в
[main.py](main.py) через `app.include_router(forecast_router)`:

| Эндпоинт | Источник | Покрытие |
|---|---|---|
| `/forecast/em/{symbol}` | Eastmoney (东财) | только A-share |
| `/forecast/ths/{symbol}` | Tonghuashun/同花顺 | только A-share |
| `/forecast/hk/{symbol}` | ET Net/经济通 | только HK (BOC/CCB/CITIC и т.п.) |
| `/forecast/aggregate/{symbol}` | все применимые | определяется по формату кода |

Авторизация — тот же Bearer-токен, что и у остальных эндпоинтов; логика
`check_auth` продублирована в `forecast_endpoints.py`, а не импортирована
из `main.py`, чтобы не создавать циклический импорт (`main.py` сам
импортирует роутер из этого файла).

## Нюансы, на которые напоролись при деплое

1. **Dockerfile не копировал новый файл.**
   `COPY main.py .` копировал только один файл — `forecast_endpoints.py`
   в образ не попадал, импорт падал при старте контейнера
   (`ModuleNotFoundError`), Render показывал "Exited with status 1".
   Фикс: `COPY main.py forecast_endpoints.py .`

2. **`inf`/`-inf` в данных ET Net ломали JSON-сериализацию.**
   `pandas.DataFrame.where(df.notnull(), None)` заменяет только `NaN`, но
   не `inf`/`-inf` — `json.dumps` падает с `ValueError: Out of range float
   values are not JSON compliant`, FastAPI отдаёт голый `500` без тела.
   В `main.py` (`df_response`) это уже было учтено через
   `df.replace([float("inf"), float("-inf")], None)` — в новом роутере
   этот шаг изначально пропустили. Исправлено по тому же паттерну в
   `_df_to_records`.

3. **Eastmoney не фильтрует по тикеру.**
   `ak.stock_profit_forecast_em` фильтрует только по отраслевому сегменту,
   не по коду — приходится тянуть всю таблицу и фильтровать на своей
   стороне. Чтобы не гонять пагинацию на каждый запрос — TTL-кэш в памяти
   на 6 часов (`_em_cache`), общий на процесс (не Redis — для одного
   инстанса на Render этого достаточно).

4. **У THS/ET Net `indicator` должен совпадать ТОЧНО.**
   Частичные или произвольные строки `indicator` у обоих источников
   иногда молча возвращают пустой результат вместо ошибки — поэтому
   значение валидируется по списку допустимых констант
   (`_THS_INDICATORS`, `_ET_INDICATORS`) с `400`, если не совпало.

5. **Импортер схемы в ChatGPT Actions не резолвит `$ref`.**
   Первая версия `openapi_schema.yaml` переиспользовала общие параметры и
   ответы через `$ref` (как это принято в OpenAPI) — импортер GPT builder'а
   их не разворачивает и просто пропускает операцию с ошибкой вида
   `parameter {'$ref': ...} is has missing or non-string name`. Пришлось
   развернуть все `$ref` в литеральные inline-блоки по всему файлу и
   добавить пустую `components: schemas: {}` (без неё — отдельное
   предупреждение "schemas subsection is not an object").

## Как обновить кастомный GPT после изменений схемы

В builder'е GPT (chatgpt.com) → Actions → Import from URL:

```
https://raw.githubusercontent.com/Pashgun88/china-securities-proxy/main/openapi_schema.yaml
```

Репозиторий публичный, поэтому импорт по URL работает без ручного
копипаста YAML (там легко ломаются отступы). Текст `gpt_instructions.txt`
пока копируется в Instructions вручную — отдельного механизма импорта для
него нет.

## Проверено на проде (Bearer-токен, три HK-тикера)

```
GET /forecast/hk/3988   -> 200, BOC
GET /forecast/hk/939    -> 200, CCB
GET /forecast/hk/267    -> 200, CITIC
```

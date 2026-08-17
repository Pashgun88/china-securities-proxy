# China Securities Data Proxy

REST API-обёртка над Python-библиотекой [AKShare](https://akshare.akfamily.xyz/)
для доступа к данным китайских/гонконгских бирж (SSE/SZSE/HKEX) — финансовая
отчётность, дивиденды, дневные котировки, курс CNY/HKD. Предназначена для
подключения к Custom GPT в ChatGPT через Actions.

## Локальный запуск

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export PROXY_ACCESS_KEY=mysecret   # опционально, для локальной разработки можно не задавать
uvicorn main:app --reload --port 8000
```

Проверка:

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/daily?ts_code=601998&start_date=2024-12-01&end_date=2024-12-31"
```

Если `PROXY_ACCESS_KEY` задан, запросы должны содержать заголовок:

```
Authorization: Bearer mysecret
```

## Docker

```bash
docker build -t china-securities-proxy .
docker run -p 8000:8000 -e PROXY_ACCESS_KEY=mysecret china-securities-proxy
```

## Эндпоинты

| Метод | Путь | Описание |
|---|---|---|
| GET | /health | проверка живости |
| GET | /date | текущая дата сервера (нужна GPT: своих часов у модели нет) |
| GET | /income | отчёт о прибылях и убытках (A-share) |
| GET | /balancesheet | баланс (A-share) |
| GET | /cashflow | движение денежных средств (A-share) |
| GET | /dividend | история дивидендов (A-share) |
| GET | /daily | дневные котировки (A-share) |
| GET | /hk_daily | дневные котировки (HK) |
| GET | /hk_financial | финансовые показатели (HK) |
| GET | /fx | курс валюты к CNY (Bank of China) |
| GET | /fx_cny_hkd_on_date | курс CNY/HKD на конкретную дату (BOC) |
| GET | /stock_basic | базовая информация о тикере |
| GET | /forecast/em/{symbol} | прогноз консенсуса Eastmoney (A-share) |
| GET | /forecast/ths/{symbol} | прогноз консенсуса Tonghuashun/同花顺 (A-share) |
| GET | /forecast/hk/{symbol} | прогноз консенсуса ET Net/经济通 (HK) |
| GET | /forecast/hk_brokers/{symbol} | индивидуальные прогнозы брокеров ET Net (HK) — брокер, дата, FY, EPS, DPS, TP, рейтинг |
| GET | /forecast/aggregate/{symbol} | все применимые источники прогнозов за один вызов |
| GET | /consensus/aastocks/{symbol} | консенсус-прогнозы брокеров (HK), парсинг AASTOCKS.com — **manual/low-frequency use only** |

Подробности по прогнозам — фильтр актуальности, окно свежести, разметка
выбросов — см. [FORECAST_NOTES.md](FORECAST_NOTES.md).

### /forecast/hk_brokers/{symbol}

Отвечает на вопрос «кто и когда дал прогноз», в отличие от обезличенного
консенсуса `/forecast/hk`. Источник — индикатор ET Net `盈利预测概览`.

```bash
curl "http://localhost:8000/forecast/hk_brokers/01810?max_age_days=30&min_brokers=10"
```

Окно свежести: берётся `max_age_days`; если уникальных брокеров меньше
`min_brokers` — окно расширяется по ступеням (60/90/180/365 дней), и
фактически применённое возвращается в `window_used_days`. Считаются именно
уникальные брокеры, а не строки: один дом даёт по строке на каждый
финансовый год, поэтому по строкам порог набирается ложно.

Если покрытия не хватает даже без ограничения по дате, ответ содержит
`coverage_exhausted: true` — данные при этом отдаются полностью. Так, у
Bank of China 3988 аналитиков всего 8, и десять не наберётся ни при каком
окне; пустой ответ здесь был бы бесполезен.

Поле `is_outlier` помечает прогнозы, статистически выбивающиеся из
остальных (отклонение от среднего более чем вдвое больше следующего по
величине, отдельно внутри каждого финансового года). Строки не удаляются:
списка «надёжных» домов у сервиса нет, поэтому решение принимает человек —
имя брокера и дата есть в каждой строке.

### /consensus/aastocks/{symbol}

Manual/low-frequency use only — not for automated polling. Респектует ToS
AASTOCKS.com тем, что ограничивается редкими ручными запросами (ожидается
1-2 запуска в год на несколько тикеров), а не постоянным опросом. Не
подключён к keep-warm workflow (`.github/workflows/keep-warm.yml`) и не
вызывается автоматически ни одним другим эндпоинтом или воркфлоу проекта —
запускать только вручную. Подробности парсинга и его ограничения — см.
docstring в [broker_consensus.py](broker_consensus.py).

## Деплой

Сервис задеплоен на Render.com (`https://china-securities-proxy.onrender.com`)
с автодеплоем из ветки `main` — push в `main` запускает пересборку образа
сам, вручную ничего нажимать не нужно. Сборка идёт по [Dockerfile](Dockerfile);
новые модули обязательно добавлять в строку `COPY`, иначе контейнер упадёт
с `ModuleNotFoundError` уже на старте.

`PROXY_ACCESS_KEY` задаётся в переменных окружения Render и в репозиторий не
попадает. `/health` и `/date` намеренно оставлены без авторизации — первый
нужен воркфлоу прогрева [keep-warm.yml](.github/workflows/keep-warm.yml)
(free tier усыпляет сервис после простоя), второй ничего чувствительного не
отдаёт.

После изменения [openapi_schema.yaml](openapi_schema.yaml) схему нужно
переимпортировать в кастомный GPT вручную — см. раздел в
[FORECAST_NOTES.md](FORECAST_NOTES.md).

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
| GET | /consensus/aastocks/{symbol} | консенсус-прогнозы брокеров (HK), парсинг AASTOCKS.com — **manual/low-frequency use only** |

### /consensus/aastocks/{symbol}

Manual/low-frequency use only — not for automated polling. Респектует ToS
AASTOCKS.com тем, что ограничивается редкими ручными запросами (ожидается
1-2 запуска в год на несколько тикеров), а не постоянным опросом. Не
подключён к keep-warm workflow (`.github/workflows/keep-warm.yml`) и не
вызывается автоматически ни одним другим эндпоинтом или воркфлоу проекта —
запускать только вручную. Подробности парсинга и его ограничения — см.
docstring в [broker_consensus.py](broker_consensus.py).

## Деплой

Инструкции по деплою на Render.com — см. сообщение ассистента / историю чата.

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
| GET | /forecast/revenue/{symbol} | прогноз **выручки** (Yahoo Finance) — единственный источник выручки, горизонт 2 года |
| GET | /forecast/eps_scale/{symbol} | во сколько делить EPS/DPS из ET Net — вычисляется сверкой с Yahoo |
| GET | /forecast/bvps/{symbol} | прогнозный BVPS расчётом clean surplus (прямого источника не существует) |
| GET | /forecast/fx_forward | будущий курс CNY/HKD — рыночный форвард CFETS, горизонт 1 год |
| GET | /forecast/aggregate/{symbol} | все применимые источники прогнозов за один вызов |
| GET | /consensus/aastocks/{symbol} | свежие пересмотры прогнозов брокеров (HK), парсинг AASTOCKS.com — **троттлится** |

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

### Выручка, BVPS и курс — чего нет у ET Net

ET Net (основной источник прогнозов по HK) не публикует ни прогноза
выручки, ни BVPS, ни курса — это проверено по всем четырём его
индикаторам, а не предположено. Три эндпоинта закрывают эти пробелы
разными способами, и у каждого своя честная граница применимости,
которая возвращается прямо в ответе:

| Показатель | Как получен | Горизонт |
|---|---|---|
| Выручка | консенсус Yahoo Finance (S&P Global / TipRanks) | 2 года, дальше бесплатно не публикует никто |
| BVPS | расчёт clean surplus поверх прогнозных EPS/DPS | пока есть прогноз EPS, с падающим `confidence` |
| Курс CNY/HKD | форвард, вменённый рынком (спот CFETS + своп-пункты) | 1 год, дальше CFETS не котирует |

Ни один из них нельзя экстраполировать за пределы горизонта — правила
18m-18o в [gpt_instructions.txt](gpt_instructions.txt) прямо это
запрещают. Подробности, включая обе валютные ловушки Yahoo и
перекрёстную проверку конвенции своп-пунктов, — в
[FORECAST_NOTES.md](FORECAST_NOTES.md) и docstring
[market_estimates.py](market_estimates.py).

Отдельно: `/forecast/eps_scale/{symbol}` отвечает на вопрос, во сколько
раз делить EPS/DPS из ET Net (он отдаёт их в сотых долях валюты). Раньше
это была инструкция «сравни порядок величины на глаз»; теперь множитель
вычисляется сверкой с независимым консенсусом Yahoo и возвращается вместе
с доказательствами — по каким финансовым годам и с каким отношением.

### /consensus/aastocks/{symbol}

Отвечает на вопрос «что аналитики поменяли за последние дни»: рейтинг и
целевая цена из research-заметок AASTOCKS.com. Это **события пересмотра**, а
не таблица абсолютных прогнозов по годам — дополняет `/forecast/hk_brokers`,
а не заменяет его.

Официального API у источника нет, страницы парсятся, поэтому нагрузка
ограничивается осознанно: кэш на тикер (6 часов), суточный потолок живых
обходов (20, при исчерпании — `429`), пауза между статьями внутри обхода.
Не подключён к keep-warm workflow (`.github/workflows/keep-warm.yml`) и не
вызывается по расписанию — только в ответ на живой запрос пользователя.

Извлечение эвристическое: имя брокера, рейтинг и TP берутся из заголовка
заметки (`Nomura Raises TENCENT TP from HKD650 to HKD690, Maintains
Neutral` → `Nomura` / `Buy`-`Neutral` / `650 -> 690`), недостающее
добирается из текста статьи. В каждой записи возвращаются `headline`,
`raw_snippet` и `source_url` — по ним результат можно проверить. Подробности
и ограничения — docstring в [broker_consensus.py](broker_consensus.py).

Проверить парсинг на текущей вёрстке сайта:

```bash
python3 test_aastocks_consensus.py   # живые запросы, 3 тикера
```

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

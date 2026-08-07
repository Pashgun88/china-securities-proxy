"""
test_aastocks_consensus.py

Ручной прогон fetch_aastocks_consensus() по трём тикерам (BOC/CCB/CITIC) —
запускать вручную перед коммитом, чтобы убедиться, что парсинг реально
вытаскивает данные с текущей вёрстки AASTOCKS.com:

    python3 test_aastocks_consensus.py

Не тест в смысле pytest/CI — намеренно, см. предупреждение в
broker_consensus.py про manual/low-frequency use only.
"""

import json

from broker_consensus import fetch_aastocks_consensus

TICKERS = {
    "03988": "BOC",
    "00939": "CCB",
    "00267": "CITIC",
}


def main():
    for symbol, name in TICKERS.items():
        print(f"\n{'=' * 60}\n{name} ({symbol})\n{'=' * 60}")
        try:
            result = fetch_aastocks_consensus(symbol)
        except Exception as exc:
            print(f"FAILED: {type(exc).__name__}: {exc}")
            continue
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

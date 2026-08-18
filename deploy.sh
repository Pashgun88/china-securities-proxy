#!/usr/bin/env bash
# Выкатка на VPS: забрать код из main и перезапустить сервис.
#
# Автодеплоя по push здесь нет намеренно — сервис живёт на своей машине, и
# тихая выкатка непроверенного коммита на боевой Action, которым пользуется
# GPT, опаснее лишней ручной команды.
#
# Использование:  ./deploy.sh
set -euo pipefail

cd "$(dirname "$0")"

echo "==> git pull"
git pull --ff-only origin main

# Зависимости обновляем только если менялся requirements.txt — иначе каждая
# выкатка тратит минуту на проверку уже установленного.
if ! git diff --quiet HEAD@{1} HEAD -- requirements.txt 2>/dev/null; then
    echo "==> requirements.txt изменился, обновляю зависимости"
    ./venv/bin/python3 -m pip install -q -r requirements.txt
fi

echo "==> проверка синтаксиса до рестарта"
for f in main.py forecast_endpoints.py errors.py broker_consensus.py market_estimates.py; do
    ./venv/bin/python3 -m py_compile "$f"
done

echo "==> restart"
sudo systemctl restart china-securities-proxy

# Ждём, пока сервис реально начнёт отвечать, а не просто перейдёт в active:
# systemd считает запуск успешным до того, как uvicorn поднимет приложение.
echo -n "==> жду готовности"
for _ in $(seq 1 30); do
    if curl -sf -m 3 -o /dev/null http://127.0.0.1:8010/health; then
        echo " — готов"
        curl -s -m 10 https://china.paveln8n.cloud/health
        echo
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo
echo "СЕРВИС НЕ ОТВЕЧАЕТ после рестарта. Логи:"
sudo journalctl -u china-securities-proxy -n 30 --no-pager
exit 1

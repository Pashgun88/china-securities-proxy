#!/usr/bin/env bash
# Сторожевая проверка china-securities-proxy.
#
# Проверяем ПУБЛИЧНЫЙ адрес, а не localhost:8010 намеренно: сервис может быть
# жив, а наружу не отвечать из-за nginx, сертификата или firewall - именно так
# и выглядел бы отказ со стороны ChatGPT. Проверка localhost такое пропустит.
#
# Всё пишется в journal сервиса-таймера: смотреть
#   sudo journalctl -u china-proxy-watchdog -n 50
set -uo pipefail

URL="https://china.paveln8n.cloud/health"
ATTEMPTS=3
CERT_WARN_DAYS=20

ok=0
for i in $(seq 1 "$ATTEMPTS"); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$URL" || echo "000")
    if [ "$code" = "200" ]; then
        ok=1
        break
    fi
    echo "попытка $i/$ATTEMPTS: HTTP $code"
    # Пауза только между попытками: единичный сбой бывает при выкатке.
    [ "$i" -lt "$ATTEMPTS" ] && sleep 10
done

if [ "$ok" -ne 1 ]; then
    echo "СЕРВИС НЕ ОТВЕЧАЕТ по $URL после $ATTEMPTS попыток — перезапускаю"
    systemctl restart china-securities-proxy

    # Даём подняться и проверяем, помогло ли: перезапуск, который не помог,
    # важнее самого факта падения - значит проблема не в процессе.
    sleep 15
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$URL" || echo "000")
    if [ "$code" = "200" ]; then
        echo "перезапуск помог, сервис отвечает"
    else
        echo "ПЕРЕЗАПУСК НЕ ПОМОГ (HTTP $code) — нужна ручная проверка nginx/сертификата/DNS"
        exit 1
    fi
fi

# Срок сертификата. Продление автоматическое (snap.certbot.renew.timer), но
# системный apt-овский certbot сломан конфликтом pyOpenSSL, и если однажды
# сработает он вместо snap - молчаливое истечение положит весь Action.
cert="/etc/letsencrypt/live/china.paveln8n.cloud/fullchain.pem"
if [ -r "$cert" ]; then
    end=$(openssl x509 -enddate -noout -in "$cert" 2>/dev/null | cut -d= -f2)
    if [ -n "$end" ]; then
        left=$(( ( $(date -d "$end" +%s) - $(date +%s) ) / 86400 ))
        if [ "$left" -lt "$CERT_WARN_DAYS" ]; then
            echo "ВНИМАНИЕ: сертификат истекает через $left дн. Продлить: sudo /snap/bin/certbot renew"
            exit 1
        fi
    fi
fi

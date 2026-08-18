# Конфигурация сервера

Копии файлов, которые живут вне репозитория — чтобы после переустановки
машины не собирать их заново по памяти. Это **копии**, а не источник правды:
правка здесь ничего не меняет, нужно копировать в систему и перечитывать юниты.

| Файл здесь | Куда ставится |
|---|---|
| `china-securities-proxy.service` | `/etc/systemd/system/` |
| `china-proxy-watchdog.service` | `/etc/systemd/system/` |
| `china-proxy-watchdog.timer` | `/etc/systemd/system/` |
| `china-proxy-watchdog.sh` | `/usr/local/bin/` (нужен `chmod +x`) |
| `nginx-china-securities-proxy.conf` | `/etc/nginx/sites-available/china-securities-proxy` + симлинк в `sites-enabled` |

Чего здесь намеренно НЕТ: `/etc/china-securities-proxy.env` с
`PROXY_ACCESS_KEY`. Секрет в репозиторий не попадает — при восстановлении
создать файл заново с правами `600` и вписать значение из билдера GPT.

Развернуть с нуля:

```bash
sudo cp ops/china-*.service ops/china-*.timer /etc/systemd/system/
sudo cp ops/china-proxy-watchdog.sh /usr/local/bin/ && sudo chmod +x /usr/local/bin/china-proxy-watchdog.sh
sudo cp ops/nginx-china-securities-proxy.conf /etc/nginx/sites-available/china-securities-proxy
sudo ln -sf /etc/nginx/sites-available/china-securities-proxy /etc/nginx/sites-enabled/
sudo systemctl daemon-reload
sudo systemctl enable --now china-securities-proxy china-proxy-watchdog.timer
sudo nginx -t && sudo systemctl reload nginx
sudo /snap/bin/certbot --nginx -d china.paveln8n.cloud   # именно snap: системный certbot сломан
```

Конфиг nginx в этой копии — уже с TLS-секцией, которую дописал certbot.
При развёртывании на чистой машине сертификата ещё нет, и nginx не
стартует с несуществующими путями: положить конфиг без TLS-строк, выпустить
сертификат, certbot допишет их сам.

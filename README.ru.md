# mytonprovider-backend

Backend сервис для mytonprovider.org — сервис мониторинга провайдеров TON Storage.

## Описание

Данный backend:
- Обнаруживает провайдеров TON Storage, сканируя историю транзакций мастер-контракта
- Мониторит доступность провайдеров, проводит проверки здоровья через ADNL протокол
- Верифицирует хранимые файлы (скачивает случайный кусок бэга и проверяет его Merkle-доказательство)
- Обрабатывает телеметрию от провайдеров
- Вычисляет рейтинг, аптайм, статус провайдеров
- Предоставляет REST API эндпоинты для фронтенда
- Собирает метрики через **Prometheus**

## Архитектура

Система состоит из двух отдельных бинарников:

| Бинарник | Роль |
|---|---|
| **coordinator** | Один экземпляр. Управляет всем состоянием в БД, курсорами, вызовами TON lite-client и оркестрацией. |
| **agent** | N экземпляров. Stateless ADNL/DHT/RLDP воркер. Без подключения к БД. |

Агенты регистрируются у координатора при запуске и отправляют heartbeat каждые 30 с. Координатор распределяет пинг провайдеров и проверку хранилищ между зарегистрированными агентами, агрегирует результаты и записывает их в базу данных.

Оба бинарника используют `INTERNAL_TOKEN` как общий секрет для внутреннего HTTP API (заголовок `X-Internal-Token`). Оставьте пустым для отключения аутентификации (только локальная разработка).

## Установка и настройка

### Сервер координатора (Debian 12)

1. **Пробрасываем ключи с локальной машины**

```bash
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/init_server_connection.sh
USERNAME=root PASSWORD=supersecretpassword HOST=123.45.67.89 bash init_server_connection.sh
```

2. **Заходим на удаленную машину и качаем скрипт установки**

```bash
ssh root@123.45.67.89
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/setup_server.sh
```

3. **Запускаем настройку и установку**

```bash
PG_USER=pguser PG_PASSWORD=secret PG_DB=providerdb \
NEWFRONTENDUSER=jdfront \
NEWSUDOUSER=johndoe NEWUSER_PASSWORD=newsecurepassword \
INTERNAL_TOKEN=$(openssl rand -hex 32) \
bash ./setup_server.sh
```

По завершении выведет полезную информацию по использованию сервера.

### Сервер агента (Debian 12)

Запускается на каждом дополнительном сервере для ADNL-работы:

```bash
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/setup_agent.sh
COORDINATOR_URL=http://<coordinator-host> \
TON_CONFIG_URL=https://ton-blockchain.github.io/global.config.json \
INTERNAL_TOKEN=<shared-secret> \
NEWSUDOUSER=agentuser \
bash ./setup_agent.sh
```

Затем открыть ADNL-порт при необходимости (по умолчанию 16168):
```bash
ufw allow 16168/udp
```

## Разработка

### Env файлы

| Файл | Назначение |
|---|---|
| `.postgres.env` | Данные для подключения к Postgres (`docker compose` и `scripts/init_db.sh`) |
| `.coordinator.env` | Все переменные окружения координатора |
| `.agent.env` | Все переменные окружения агента |

### База данных

```bash
docker compose up -d

# Сброс (удаляет все данные)
docker compose down -v && docker compose up -d
```

### Запуск локально

```bash
# Координатор
env $(grep -v '^#' .coordinator.env | xargs) go run ./cmd/coordinator

# Агент (в отдельном терминале)
env $(grep -v '^#' .agent.env | xargs) go run ./cmd/agent
```

Флаг `-tags=debug` для координатора включает CORS-заголовки и обработку OPTIONS (нужно при работе без nginx):

```bash
env $(grep -v '^#' .coordinator.env | xargs) go run -tags=debug ./cmd/coordinator
```

### Конфигурация VS Code

Создайте `.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Coordinator",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/coordinator",
            "buildFlags": "-tags=debug",
            "env": {}
        },
        {
            "name": "Agent",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/agent",
            "env": {}
        }
    ]
}
```

## Структура проекта

```
cmd/
├── coordinator/       # Бинарник координатора (БД, TON, оркестрация)
└── agent/             # Бинарник агента (ADNL/DHT/RLDP)
pkg/
├── agentClient/       # HTTP клиент для вызовов координатор→агент
├── agentRegistry/     # Реестр агентов с вытеснением по heartbeat
├── agentServer/       # HTTP хандлеры агента и ADNL/proof-check воркеры
├── clients/           # Внешние клиенты (TON lite-client, ifconfig.co)
├── httpServer/        # Fiber хандлеры (публичный API + внутренние роуты агентов)
├── models/            # Модели для БД и API
├── repositories/      # Все запросы к Postgres
├── services/          # Бизнес-логика (поиск провайдеров, телеметрия)
└── workers/           # Харнесс воркеров и сами воркеры
db/                    # init.sql — единственный файл миграции
scripts/               # Скрипты установки и утилиты
```

## Лицензия

Apache-2.0

Этот проект был создан по заказу участника сообщества TON Foundation.

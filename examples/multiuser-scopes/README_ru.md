# Анонимные пользователи и прикладные зоны — FastAPI

Пример использует публичные пакеты MFUP/3, анонимную cookie и общий React-клиент. Личность, выбор зоны и корни назначения — правила приложения в authorize. Scope является обычным полем meta.

Из корня репозитория после установки зависимостей и сборки:

```bash
python -m uvicorn app:create_example --factory --app-dir examples/multiuser-scopes/server --port 3001
```

Backend предоставляет MFUP adapter под /api и прикладные маршруты /api/whoami, /api/files/{scope}, /api/file/{scope}. Личность проверяется при загрузке, listing и download. Жизненный цикл управляет очисткой сессий. Клиент, Docker, расположение данных и настройки описаны в [руководстве примеров](../README_ru.md).

[English](README.md)

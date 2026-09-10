# Анонимные пользователи и прикладные зоны — Node

Пример использует публичные пакеты MFUP/3, анонимную cookie и общий React-клиент. Личность, выбор зоны и корни назначения — правила приложения в authorize. Scope является обычным полем meta.

Из корня репозитория после установки зависимостей и сборки:

```bash
node examples/multiuser-scopes-node/server/server.mjs
```

Backend предоставляет MFUP adapter под /api и прикладные маршруты /api/whoami, /api/files/{scope}, /api/file/{scope}. Личность проверяется при загрузке, listing и download. Жизненный цикл управляет очисткой сессий. Клиент, Docker, расположение данных и настройки описаны в [руководстве примеров](../README_ru.md).

[English](README.md)

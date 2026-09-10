# Примеры интеграции

Node и FastAPI используют публичные пакеты MFUP/3 и общий React-клиент. Политика приложения реализована через authorize и mapFile/map_file.

| Пример                                                      | Backend                         |
| ----------------------------------------------------------- | ------------------------------- |
| [multiuser-scopes-node](multiuser-scopes-node/README_ru.md) | Node HTTP/WebSocket adapter     |
| [multiuser-scopes](multiuser-scopes/README_ru.md)           | FastAPI router и жизненный цикл |

Из корня репозитория docker compose up -d запускает Node на http://localhost:20060 и Python на http://localhost:20060/python/.

Локально: npm ci --no-audit --no-fund, npm run build, npm run dev. UI — http://localhost:3000, Node backend — 127.0.0.1:3001. Для Python установите ./server/mfup-core и ./server/mfup-fastapi, задайте MFUP_BACKEND=python. В PowerShell: $env:MFUP_BACKEND = 'python'. PYTHON_BIN выбирает интерпретатор, PORT — UI, EXAMPLE_API_PORT — backend, MFUP_EXAMPLE_DATA — каталог данных.

## Политика приложения

Браузер получает анонимную HttpOnly, SameSite=Lax cookie со случайным ключом. Отображается производный user ID. У пользователя есть зоны workspace, scratch, uploads. Это значения meta.scope в примере, а не поля протокола. Authorize проверяет личность и зону, задаёт targetDir=<uid>/<scope>, квоты 20 000 файлов/512 МиБ и серверный context. Запрошенный браузером targetDir не заменяет эту схему.

ScopeRoots (Node) / scope_roots (Python) выбирает абсолютный корень известной прикладной зоны. Загрузка, listing и download используют одно правило и публичный publishedDirectory/published_directory. Результат: <baseDir>/published/<uid>/<scope>/<mapped path>. По умолчанию Compose использует data/example-node и data/example-python. Один процесс владеет каталогом метаданных; Redis не требуется.

Прикладные endpoints: /api/whoami, /api/files/{scope}?path=..., /api/file/{scope}?path=.... Личность и зона проверяются при чтении и загрузке. Удаление cookie теряет доступ через эту анонимную личность; восстановление учётной записи в пример не входит.

## Интерфейс загрузки

В каждой зоне доступны файлы, папки, drag/drop, pause/resume, одно разрешение перезаписи всей сессии или отмена и единичная операционная ошибка. Ticket хранится отдельно по backend/user/zone. После reload нужно выбрать тот же источник. XHR-полоса показывает отправленный и подтверждённый payload, максимум 99% до публикации. Передача может продолжаться до разрешения.

Для серверной обработки передайте clientPublish:false, onCommitted:handler в Node factory либо client_publish=False, on_committed=handler в Python. Handler читает listStaged/openStaged и возвращает true после обработки. Meta и context доступны в CommitEvent. См. [API расширения](../docs/ru/EXTENDING.md) и [протокол](../docs/ru/PROTOCOL.md).

Npm run test:examples проверяет UI. Unit/HTTP tests проверяют владельцев, listing/download, корни сессий, маппинг и restart. Проверки установленных пакетов копируют эти серверные примеры в отдельный проект и выполняют их через собранные дистрибутивы.

[English](README.md)

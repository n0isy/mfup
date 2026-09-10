# Тестирование

Репозиторий использует одинаковый HTTP-контракт и браузерные сценарии для Node и Python. CI работает на Linux, Windows и macOS; конфигурация: [.github/workflows/ci.yml](../../.github/workflows/ci.yml).

## Локальные команды

Требуются Node 22.13+ и Python 3.10+:

```bash
npm ci --no-audit --no-fund
npm run build
npm run check
npm test
python -m pip install ./server/mfup-core './server/mfup-fastapi[dev]'
python -m pytest server/tests -q
npm run pack:check
npx playwright install --with-deps chromium firefox webkit chrome msedge
```

Linux/macOS:

```bash
MFUP_EXTENDED=1 npm run test:e2e
MFUP_EXTENDED=1 MFUP_BACKEND=python PYTHON_BIN=python npm run test:e2e
MFUP_EXTENDED=1 npm run test:examples
MFUP_EXTENDED=1 MFUP_BACKEND=python PYTHON_BIN=python npm run test:examples
```

В PowerShell переменные задаются отдельно: например $env:MFUP_EXTENDED = '1' и $env:MFUP_BACKEND = 'python', затем те же npm-команды.

Протокольные тесты используют локальные порты 20063–20065, примеры — 20067–20068. Supervisors запускают и останавливают backend. Данные находятся в игнорируемых .tmp-каталогах. PYTHON_BIN выбирает интерпретатор. Без MFUP_EXTENDED запускаются Chromium, Firefox, WebKit; с ним добавляются Chrome/Edge, эмуляции Pixel 7 и iPhone 13.

## Покрытие

- Постепенный обход с ограниченной очередью, пустые каталоги, группировка файлов и шесть активных запросов.
- Native File/Blob без чтения arrayBuffer приложением; точные итоговые байты.
- События внутри длинных POST до receipts, pause/resume и счётчики подтверждения.
- Усечённые запросы, потерянные receipts, решение о повторе, epochs и проверка диапазонов.
- Restart, сохранённые meta и планы, восстановление частичной публикации.
- Одно разрешение перезаписи, подтверждение/отмена во время и после приёма, параллельная отмена и поздние ответы.
- Cookie приложения, собственные listing/download, корни и hooks через установленные пакеты.
- Квоты, коллизии путей/регистра, координация sweeper, один владелец БД.
- Ошибка обработки, явный retry, политика публикации и отмена callback.

Реальные файловые сценарии запускаются в Linux Docker: npm run test:storage. Непривилегированные контейнеры проверяют заполненный tmpfs 4 МиБ, read-only mount и отсутствие прав записи. После освобождения места проверяются resume и содержимое. SQLite FULL воспроизводится лимитом страниц. EIO проверяется управляемым отказом файлового вызова. Физические неисправности, потеря питания и реальные мобильные устройства этими проверками не моделируются.

Firefox Playwright — сборка для автоматизации. WebKit и эмуляция iPhone не подтверждают поведение штатного Safari или физического iPhone. См. [браузеры Playwright](https://playwright.dev/docs/browsers).

## Проверки дистрибутивов

Npm run pack:check собирает tarballs, устанавливает во временный проект, проверяет содержимое и запускает копию consumer-примера. Python wheels устанавливаются перед python scripts/pack-python-check.py; метаданные wheel/sdist проверяет twine. Scripts/check-artifacts.py ограничивает разрешённые файлы и ищет строки, похожие на ключи, и кириллицу вне переведённых README.

CI содержит unit/packaging-матрицу Node 22.13/Python 3.10 и Node 24/Python 3.12 на трёх ОС. Отдельная browser-матрица проверяет оба backend и семь конфигураций на каждой ОС. Логи, traces и screenshots сохраняются в CI artifacts. Npm audit в набор не входит.

[English](../TESTING.md)

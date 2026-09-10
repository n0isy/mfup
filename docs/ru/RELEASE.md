# Выпуск пакетов

Пять дистрибутивов используют версию **3.1.0**:

| npm          | Python       |
| ------------ | ------------ |
| @mfup/client | mfup-core    |
| @mfup/react  | mfup-fastapi |
| @mfup/server |              |

Python-импорты: mfup_core и mfup_fastapi. Идентификатор протокола — MFUP/3. Метаданные репозитория и пакетов указывают на https://github.com/n0isy/mfup.

## Сборка и проверка

```bash
npm ci --no-audit --no-fund
npm run build
npm run check
npm test
npm run pack:check
python -m pip install build twine ./server/mfup-core './server/mfup-fastapi[dev]'
python -m pytest server/tests -q
mkdir -p dist-npm dist-py
npm pack -w @mfup/client -w @mfup/react -w @mfup/server --pack-destination dist-npm
python scripts/build-python.py dist-py
twine check dist-py/*
python scripts/check-artifacts.py dist-npm dist-py
python scripts/check-version.py 3.1.0
```

Npm tarballs содержат скомпилированный JavaScript, объявления TypeScript, метаданные, лицензию и README на двух языках. Python wheels содержат модуль, py.typed, метаданные/лицензию; source tarballs также содержат pyproject.toml и оба README. Тесты, demo, бенчи, загруженные данные, логи, окружения разработки и локальные отчёты исключены.

## CI/CD

[Release.yml](../../.github/workflows/release.yml) запускается тегами v3.* или вручную. Сначала выполняется полная переиспользуемая CI-матрица. Build job проверяет версии, собирает и валидирует архивы, сохраняет npm/Python artifacts.

Публикуют tag-запуски. Ручной запуск с artifact_run_id повторяет публикацию существующего релизного запуска. Source job требует успешную сборку и проверки, доступные npm/Python artifacts и тег, указывающий на коммит их исходников. Обычный ручной запуск собирает предварительные архивы. Npm/PyPI jobs используют окружение GitHub release и OIDC с id-token:write. Идентификатор существующего trusted publisher: репозиторий n0isy/mfup, workflow release.yml, environment release. Пароли реестров в репозитории не хранятся. Npm использует Node 24 и npm 11; стабильные пакеты публикуются в latest с provenance. Существующие версии при повторе пропускаются. PyPI использует pypa/gh-action-pypi-publish с пропуском существующих файлов.

Verification job обращается к публичным реестрам, проверяет все версии и npm latest, устанавливает пакеты из реестров и запускает независимые consumer-примеры. Публикация подтверждается этим job, а не только успешной сборкой.

Требования реестров: [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) и [PyPI publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

[English](../RELEASE.md)

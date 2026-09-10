# mfup-fastapi

```python
from mfup_fastapi import MfupConfig, create_app

app = create_app(
    MfupConfig(
        base_dir="./state",
        authorize=my_authorize,
        prefix="/api",
        client_publish=False,
        on_committed=my_processor,
    )
)
```

Приложение предоставляет my_authorize/my_processor, личность и правила доступа. Авторизация может вернуть абсолютный корень сессии, targetDir, квоты и context. Прикладные значения вроде scope передаются через meta. Multipart переносит файлы, WebSocket — вопросы и снимки.

Для встраивания MfupEngine(config).router и lifespan (либо startup/shutdown) управляют маршрутами и очисткой. Один lifecycle на Engine и один процесс на каталог метаданных. Create_app применяет config.prefix и управляет lifecycle.

Map_file вызывается при получении манифеста до чтения тела и возвращает относительный путь либо None. On_committed получает CommitEvent и возвращает boolean для server auto_publish (default False). Client_publish=False оставляет публикацию backend. Engine предоставляет get_session, list_staged, open_staged, prepare_publish, publish, retry_committed. Ошибка callback сохраняет принятые файлы и успешный commit с processing=failed.

Standalone: MFUP_AUTHORIZE=myapp.hooks:authorize python -m mfup_fastapi. MfupConfig.from_env использует те же MFUP_* настройки, что Node; Python hooks — callable или строки module:attribute.

[API расширения](https://github.com/n0isy/mfup/blob/main/docs/ru/EXTENDING.md) · [Протокол](https://github.com/n0isy/mfup/blob/main/docs/ru/PROTOCOL.md)

[English](README.md)

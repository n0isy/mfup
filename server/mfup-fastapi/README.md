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

The application supplies identity, scopes and authorize. It may return an
absolute per-session baseDir, targetDir, quotas and context. The core remains
independent of application scope names. Ordinary multipart carries files;
WebSocket carries questions and snapshots.

For embedding, `MfupEngine(config).router` and `.lifespan` (or startup/shutdown)
manage routing and cleanup. Use one lifecycle per Engine and one process per
metadata directory. `create_app` applies config.prefix and owns the lifecycle.

map_file runs during manifest reception, before body reads and returns a relative path or None.
on_committed receives CommitEvent and can return a boolean to override server
auto_publish (default False). client_publish=False reserves publication for
the backend. Engine exposes get_session, list_staged, open_staged,
prepare_publish, publish and retry_committed. Callback failure preserves the
accepted files and returns a successful commit with processing='failed'.

Standalone: `MFUP_AUTHORIZE=myapp.hooks:authorize python -m mfup_fastapi`.
MfupConfig.from_env supports the same MFUP_* configuration as the Node package;
Python hooks can be callables or module:attribute strings.
See docs/EXTENDING.md and the multiuser-scopes example in the repository.

[Extension API](https://github.com/n0isy/mfup/blob/main/docs/EXTENDING.md) · [HTTP protocol](https://github.com/n0isy/mfup/blob/main/docs/PROTOCOL.md)

[Russian](README_ru.md)

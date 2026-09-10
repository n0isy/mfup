# mfup-core

MFUP/3 engine for streamed multipart uploads, durable receipts and interactive
publication. The core supplies Engine, ProtocolError (code/status), relative_path,
published_directory, AuthRequest, AuthResult, FileMapRequest, CommitEvent and
StagedFile. Public hook types use TypedDict and the package includes py.typed.

The application chooses identity and scopes in authorize, which can return
baseDir/targetDir, quotas, context, autoPublish and clientPublish per session.
map_file maps metadata during manifest reception, before reading bodies. on_committed can
read accepted files through list_staged/open_staged and return True to publish
or False to leave server auto-publication disabled. Backend-only publication
requires client_publish=False. Callback errors are represented separately from
successful file acceptance and can be retried through retry_committed.

Payload staging and publication share the chosen session root; SQLite stays
in the Engine root. No Redis is required. Use one owning process; session
callbacks are not an exactly-once job queue. See the repository's
`docs/EXTENDING.md` for lifecycle, retries, errors and configuration details.

[Extension API](https://github.com/n0isy/mfup/blob/main/docs/EXTENDING.md) · [HTTP protocol](https://github.com/n0isy/mfup/blob/main/docs/PROTOCOL.md)

[Russian](README_ru.md)

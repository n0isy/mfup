# mfup-core

Ядро MFUP/3 для потоковой загрузки multipart, сохраняемых receipts и публикации с разрешением пользователя. Экспортирует Engine, ProtocolError(code/status), relative_path, published_directory, AuthRequest, AuthResult, FileMapRequest, CommitEvent, StagedFile. Публичные типы hooks используют TypedDict; пакет содержит py.typed.

Приложение определяет личность и прикладные поля в authorize; результат задаёт baseDir/targetDir, квоты, context, autoPublish, clientPublish для сессии. Map_file маппит метаданные при получении манифеста до чтения тела. On_committed читает принятые файлы через list_staged/open_staged и возвращает True для публикации либо False для отключения автоматической серверной публикации. Только серверная публикация требует client_publish=False. Ошибка callback отделена от успешного приёма и повторяется через retry_committed.

Staging и публикация используют выбранный корень сессии; SQLite остаётся в корне Engine. Redis не нужен. Используйте одного владельца-процесс; callbacks не являются очередью заданий с гарантией exactly-once.

[API расширения](https://github.com/n0isy/mfup/blob/main/docs/ru/EXTENDING.md) · [Протокол](https://github.com/n0isy/mfup/blob/main/docs/ru/PROTOCOL.md)

[English](README.md)

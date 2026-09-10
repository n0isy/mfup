# @mfup/react

Интеграция React 18+ с @mfup/client.

```tsx
import { useMfupUpload } from "@mfup/react";
const { start, snapshot, setOverwrite, pause, resume, retry, cancel } =
  useMfupUpload({
    serverUrl: location.origin,
    meta: { albumId: "summer" },
    trackUploadProgress: true,
  });
```

UseMfupSession(session) подписывается на внешнюю сессию. Покажите один опциональный вопрос по snapshot.overwriteRequired и вызовите setOverwrite(true) либо cancel(). Отображайте одну ошибку из snapshot.errorInfo. Пофайловых диалогов нет. Hook освобождает клиент при unmount, сохраняя серверную загрузку. Для продолжения после переходов/reload храните ticket в приложении.

Snapshot.sentBytes показывает оценку активной отправки, snapshot.confirmedBytes — подтверждения сервера. Завершённая полоса зависит от state=published. Используйте sourceFromInput/sourceFromDataTransfer из клиентского пакета в picker/drop handlers. Drop adapter вызывается синхронно внутри события.

После исчерпания автоматических повторов вызовите retry() без источника. Хук сохраняет внутри SDK ограниченный поток; не храните копию entries/File.

[API расширения](https://github.com/n0isy/mfup/blob/main/docs/ru/EXTENDING.md) · [Протокол](https://github.com/n0isy/mfup/blob/main/docs/ru/PROTOCOL.md)

[English](README.md)

# @mfup/client

Браузерный клиент MFUP/3: native FormData, до шести параллельных корзин, постепенный обход каталогов, разрешение перезаписи и продолжение диапазонов.

SDK владеет потоком: максимум 10000 ожидающих записей вместе с активными корзинами, продолжение обхода при 5000. Не создавайте и не храните дубликат списка entries/File. По умолчанию 1000 повторов с backoff до 36 секунд дают около десяти часов ожидания. После окончательной ошибки вызовите session.retry() без повторного выбора источника; SDK удерживает ограниченную очередь. sourceFromInput сам очищает input после завершения обхода.

```ts
import { MfupSession, sourceFromInput } from "@mfup/client";
const session = new MfupSession({
  serverUrl: location.origin,
  meta: { albumId: "summer" },
  trackUploadProgress: true,
});
session.subscribe(() => console.log(session.getSnapshot()));
await session.upload(sourceFromInput(input));
session.dispose();
```

Покажите один опциональный вопрос по snapshot.overwriteRequired и вызовите session.setOverwrite(true) либо session.cancel(). Разрешение сохраняется для всей сессии, будущих конфликтов и resume. Пофайловых решений и skip нет. Начальное согласие задаётся overwrite:true.

Сохраните session.exportTicket() после connect; после reload повторно выберите источник. AutoPublish:false оставляет явную публикацию. Отмена остаётся cancelling до подтверждения сервера; поздняя отмена может вернуть published. Содержимое остаётся в native File/Blob.

Одна ошибка загрузки доступна в snapshot.errorInfo: code/status/phase/retryable. Storage_full (507) и storage_unavailable (503) не вызывают автоматической повторной отправки тела. После сетевой ошибки сначала проверяется receipt. RequestTimeoutMs по умолчанию 30 секунд для управления, без этого ограничения для длинных data POST.

TrackUploadProgress:true использует XHR upload events. SentBytes оценивает подтверждённый payload и активные отправки; confirmedBytes учитывает только receipts. Уведомления объединяются каждые 50 мс. По умолчанию и при пользовательском fetch используется fetch. После прерывания неподтверждённая оценка может уменьшиться; resume опирается на receipts.

[API расширения](https://github.com/n0isy/mfup/blob/main/docs/ru/EXTENDING.md) · [Протокол](https://github.com/n0isy/mfup/blob/main/docs/ru/PROTOCOL.md)

[English](README.md)

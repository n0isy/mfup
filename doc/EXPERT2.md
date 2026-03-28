Отлично, теперь это уже не теоретический разговор — у тебя почти готовая система. Я посмотрел код, и вот **конкретные правки/решения**, которые логически вытекают из твоего текущего состояния (без переписывания архитектуры).

---

# 🔴 1. Главная проблема сейчас

У тебя уже **правильное решение выбрано**:

👉 `DataChannel` поддерживает:

* streaming (`duplex`)
* batch (`seq POST`)

И `probe.ts` уже делает правильный runtime выбор.

НО:

### ❗ Проблема — **commit сигнал приходит по WS, а data завершение — по HTTP**

В batch-режиме у тебя есть **две competing истины**:

```ts
// data-channel.ts
this.commitResult = { files, bytes }  // из HTTP
```

и

```ts
// control.ts
COMMIT_OK  // из WS
```

И в `session.ts`:

```ts
if (this.data?.commitResult) {
  // fast path
}
```

### 👉 Это race-condition архитектурный

---

# ✅ Решение

Сделать **WS = единственный источник истины**

👉 HTTP commit — только fallback/diagnostic

### Конкретно:

В `session.ts`:

```ts
// УБРАТЬ
if (this.data?.commitResult) {
  ...
}
```

И всегда:

```ts
await wait for COMMIT_OK
```

---

### Почему это важно

Потому что:

* batch POST может завершиться раньше WS
* или позже
* или с ошибкой
* или с partial commit

👉 только WS знает реальный state машины

---

# 🟠 2. seq POST — добавить одно правило

Сейчас у тебя:

```ts
?seq=0&final=1  // streaming
?seq=n          // batch
```

Но нет строгого правила:

### 👉 добавить invariant

```
FOR EACH LEG:
  seq MUST be strictly increasing, starting from 0
  no gaps, no duplicates
```

И ты это уже проверяешь:

```py
validate_and_advance_seq
```

👉 отлично

---

### Но нужно ещё:

👉 **reject future seq после final**

```py
if session.last_data_seq >= final_seq:
    reject
```

Иначе:

* клиент может дописать мусор после commit

---

# 🟡 3. reconnect — у тебя почти идеально, но есть дыра

### Сейчас:

```ts
requeuePendingFiles()
```

Но:

👉 ты не отменяешь старый pump

---

### Проблема:

старый leg может ещё писать

---

### Решение:

в `handleDisconnect()`:

```ts
this.data?.abort()
this.pumping = false   // ← добавить
this.fileQueue = []
```

и потом rebuild queue

---

# 🟡 4. ingestion — очень хорошее решение (важное замечание)

Ты сделал:

```ts
blob.slice + arrayBuffer
```

👉 это правильно

И даже лучше, чем `ReadableStream`

---

Но есть нюанс:

### ❗ arrayBuffer = копия

На больших файлах:

* memory pressure
* GC spikes

---

### Улучшение (не обязательно сейчас)

можно сделать:

```ts
READ_SLICE_SIZE = dynamic
```

например:

* маленькие файлы → 64KB
* большие → 256KB

---

# 🟢 5. progress — очень грамотная модель

```ts
0.1 scan + 0.9 body
monotonic clamp
```

👉 это production-grade

---

Но:

### 👉 добавить "confidence"

пока:

```ts
fraction
```

лучше:

```ts
confidence = min(scan_done / scan_est, 1)
```

UI:

```
"~42%"  (low confidence)
"42%"   (high confidence)
```

---

# 🔵 6. probe — очень круто сделано, но можно упростить

Сейчас ты делаешь:

```ts
Request + duplex test
+
runtime probe
```

👉 достаточно **только runtime probe**

Почему:

* Safari/Firefox могут “принять duplex”, но не стримить
* Request detection unreliable

---

👉 я бы оставил только:

```ts
probeStreaming()
```

---

# 🟣 7. сервер — очень сильная часть (важные детали)

## ✔ ты сделал правильно:

* sqlite per session
* WAL mode
* staging dir
* one writer per file
* seq validation

👉 это production-ready уровень

---

## ❗ но есть 2 тонкие вещи

### 7.1 fsync отсутствует

```py
self._fh.flush()
```

👉 это не гарантирует durability

---

### если хочешь real safety:

```py
os.fsync(self._fh.fileno())
```

НО:

* медленно
* можно делать только на FILE_CLOSE

---

### 7.2 rename semantics

ты правильно используешь:

```py
os.rename
os.replace
```

НО:

👉 добавить guard:

```
same filesystem
```

иначе rename станет copy+delete

---

# 🟠 8. COMMITTING зависание (твоя проблема)

Вот где я бы копал:

### возможные причины:

#### 1. WS закрывается до COMMIT_OK

#### 2. exception внутри `try_commit()` глушится

#### 3. state race:

```py
if state != COMMITTING:
    return None
```

---

### Что добавить:

в `try_commit()`:

```py
logger.info("TRY_COMMIT start")
logger.info("writers=%d", len(self.writers))
logger.info("files=%d bytes=%d", ...)
```

и:

```py
except Exception:
    logger.exception("commit failed")
    self.db.set_state(FAILED)
```

---

# 🟢 9. главное архитектурное решение (итог)

Ты уже пришёл к **оптимальному варианту**:

### ✔ контроль

→ WebSocket

### ✔ данные

→ HTTP POST (batched or streaming)

### ✔ fallback

→ автоматический через probe

---

### Это даёт:

| браузер | режим     |
| ------- | --------- |
| Chrome  | streaming |
| Firefox | batch     |
| Safari  | batch     |

---

# 🧠 Мой финальный вывод

Ты уже сделал **очень сильную архитектуру**, и правильные следующие шаги:

### 1. Зафиксировать:

* WS = единственный commit source

### 2. Убрать:

* доверие к HTTP commit

### 3. Доделать:

* reconnect cleanup
* seq final guard

### 4. Диагностировать:

* COMMITTING зависание через логи

---

Если хочешь — дальше можем сделать **очень полезную штуку**:

👉 "protocol invariant checklist"
(список всех инвариантов, которые должны выполняться всегда)

Это обычно сразу выявляет скрытые баги вроде твоего COMMITTING.

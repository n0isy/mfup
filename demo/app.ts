import {
  MfupSession,
  sourceFromInput,
  sourceFromDataTransfer,
  fromFiles,
  fromHandles,
  fromEntries,
  type Source,
  type Ticket,
} from "../packages/client/src/index.js";
const $ = (id: string) => document.getElementById(id)!;
const key = "mfup3-ticket";
let session: MfupSession | null = null;
const labels: Record<string, string> = {
  idle: "Ready to upload",
  connecting: "Connecting",
  uploading: "Uploading files",
  paused: "Upload paused",
  waiting: "Your approval is needed",
  committed: "Files received",
  published: "Upload complete",
  cancelled: "Upload cancelled",
  failed: "Upload failed",
};
const format = (n: number) =>
  n >= 1048576
    ? `${(n / 1048576).toFixed(1)} MiB`
    : n >= 1024
      ? `${(n / 1024).toFixed(1)} KiB`
      : `${n} B`;
function saved(): Ticket | undefined {
  try {
    return JSON.parse(localStorage.getItem(key) ?? "null") ?? undefined;
  } catch {
    return undefined;
  }
}
$("restore").hidden = !saved();
function render() {
  if (!session) return;
  const s = session.getSnapshot();
  $("status").textContent = labels[s.state];
  $("count").textContent = `${s.discovered} files`;
  ($("progress") as HTMLProgressElement).value = s.totalBytes
    ? s.state === "published"
      ? 1
      : Math.min(0.99, s.sentBytes / s.totalBytes)
    : s.state === "published"
      ? 1
      : 0;
  $("bytes").textContent =
    `${format(s.sentBytes)} sent · ${format(s.confirmedBytes)} confirmed of ${format(s.totalBytes)}`;
  $("lanes").textContent = `${s.activeRequests} of 6 requests`;
  $("error").textContent = s.error ?? "";
  ($("pause") as HTMLButtonElement).disabled = s.state !== "uploading";
  ($("resume") as HTMLButtonElement).disabled = s.state !== "paused";
  ($("cancel") as HTMLButtonElement).disabled = [
    "published",
    "cancelled",
    "failed",
  ].includes(s.state);
  if (session.ticket)
    localStorage.setItem(key, JSON.stringify(session.exportTicket()));
  if (["published", "cancelled"].includes(s.state)) {
    localStorage.removeItem(key);
    $("restore").hidden = true;
  }
  const questions = $("questions");
  questions.replaceChildren();
  for (const q of s.asks.filter((q) => q.answer === null)) {
    const card = document.createElement("article");
    card.className = "question";
    const text = document.createElement("p");
    text.textContent =
      "Allow overwriting existing files for this entire upload?";
    card.append(text);
    const actions = document.createElement("div");
    actions.className = "actions";
    for (const [choice, label] of [
      ["overwrite", "Allow overwrite"],
      ["cancel", "Cancel upload"],
    ] as const) {
      const b = document.createElement("button");
      b.textContent = label;
      b.onclick = () => void session!.answer(q.id, choice).catch(showError);
      actions.append(b);
    }
    card.append(actions);
    questions.append(card);
  }
}
function showError(error: unknown) {
  $("error").textContent = (error as Error).message;
}
async function start(source: Source) {
  session?.dispose();
  session = new MfupSession({
    ticket: saved(),
    targetDir: "uploads",
    trackUploadProgress: true,
  });
  session.subscribe(render);
  try {
    await session.upload(source);
  } catch (error) {
    showError(error);
  }
  render();
}
for (const id of ["files", "folder"])
  $(id).addEventListener("change", (event) => {
    const input = event.target as HTMLInputElement;
    const source = sourceFromInput(input);
    input.value = "";
    void start(source);
  });
$("pause").onclick = () => session?.pause();
$("resume").onclick = () => void session?.resume().catch(showError);
$("cancel").onclick = () => void session?.cancel().catch(showError);
$("forget").onclick = () => {
  localStorage.removeItem(key);
  $("restore").hidden = true;
  session?.dispose();
  session = null;
};
const zone = $("dropzone");
zone.ondragover = (event) => {
  event.preventDefault();
  zone.classList.add("over");
};
zone.ondragleave = () => zone.classList.remove("over");
zone.ondrop = (event) => {
  event.preventDefault();
  zone.classList.remove("over");
  if (event.dataTransfer)
    void start(sourceFromDataTransfer(event.dataTransfer));
};
// The exported SDK is also exposed to the dedicated browser conformance harness.
(window as any).mfup3 = {
  MfupSession,
  fromFiles,
  fromHandles,
  fromEntries,
  sourceFromInput,
  sourceFromDataTransfer,
};

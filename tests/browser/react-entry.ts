import { createElement, StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { useMfupUpload } from "../../packages/react/src/index.js";

export async function runReact(target: string) {
  const mount = document.createElement("div");
  document.body.append(mount);
  const root = createRoot(mount);
  let api: ReturnType<typeof useMfupUpload>;
  let ready!: () => void;
  const mounted = new Promise<void>((resolve) => {
    ready = resolve;
  });
  let sessions = 0;
  const transport: typeof fetch = async (input, init) => {
    if (String(input).endsWith("/mfup/sessions") && init?.method === "POST")
      sessions++;
    return fetch(input, init);
  };
  function Component() {
    api = useMfupUpload({ targetDir: target, fetch: transport });
    useEffect(() => ready(), []);
    return createElement("output", null, api.snapshot?.state ?? "idle");
  }
  root.render(createElement(StrictMode, null, createElement(Component)));
  try {
    await mounted;
    await api!.start([new File(["react"], "react.txt", { lastModified: 1 })]);
    for (let i = 0; i < 100 && api!.snapshot?.state !== "published"; i++)
      await new Promise((r) => setTimeout(r, 10));
    return {
      sessions,
      state: api!.snapshot?.state,
      bytes: api!.snapshot?.confirmedBytes,
    };
  } finally {
    root.unmount();
    mount.remove();
  }
}

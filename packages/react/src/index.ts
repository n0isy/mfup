import {
  useCallback,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  MfupSession,
  type SessionOptions,
  type Source,
  type Choice,
} from "@mfup/client";

const emptySubscribe = () => () => {};
const emptySnapshot = () => null;
export function useMfupSession(session: MfupSession | null) {
  return useSyncExternalStore(
    session?.subscribe ?? emptySubscribe,
    session?.getSnapshot ?? emptySnapshot,
    emptySnapshot,
  );
}
export function useMfupUpload(options: SessionOptions = {}) {
  const opts = useRef(options);
  opts.current = options;
  const [session, setSession] = useState<MfupSession | null>(null),
    ref = useRef<MfupSession | null>(null);
  const snapshot = useMfupSession(session);
  const start = useCallback(async (source: Source | FileList | File[]) => {
    if (
      ref.current &&
      !["published", "cancelled", "failed"].includes(
        ref.current.getSnapshot().state,
      )
    )
      throw new Error("Upload already running");
    ref.current?.dispose();
    const s = new MfupSession(opts.current);
    ref.current = s;
    setSession(s);
    await s.upload(source);
  }, []);
  useEffect(() => () => ref.current?.dispose(), []);
  return {
    session,
    snapshot,
    start,
    pause: () => ref.current?.pause(),
    resume: () => ref.current?.resume(),
    cancel: () => ref.current?.cancel(),
    setOverwrite: (value = true) => ref.current?.setOverwrite(value),
    answer: (id: string, choice: Choice) => ref.current?.answer(id, choice),
    pendingAsks: snapshot?.asks.filter((q) => q.answer === null) ?? [],
  };
}

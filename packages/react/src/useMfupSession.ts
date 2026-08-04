// Reactive view over an MfupSession via useSyncExternalStore.
//
// MfupSession implements the store contract natively (subscribe +
// getSnapshot with referentially-stable, coalesced snapshots), so this hook
// is tear-free under concurrent rendering and costs one subscription total —
// not one per consumed field.

import { useCallback, useSyncExternalStore } from "react";
import type { MfupSession, MfupSessionSnapshot } from "@mfup/client";

const noopUnsubscribe = () => {};

/**
 * Subscribe to a session's state snapshot. Accepts null (before an upload
 * starts) and returns null in that case — and on the server (SSR), where no
 * session can exist.
 */
export function useMfupSession(session: MfupSession | null): MfupSessionSnapshot | null {
  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      session ? session.subscribe(onStoreChange) : noopUnsubscribe,
    [session],
  );
  const getSnapshot = useCallback(
    () => (session ? session.getSnapshot() : null),
    [session],
  );
  return useSyncExternalStore(subscribe, getSnapshot, () => null);
}

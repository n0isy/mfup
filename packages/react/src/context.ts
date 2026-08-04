// MFUP/2 React context — app-wide defaults so hooks can omit serverUrl etc.

import { createContext, createElement, useContext, type ReactNode } from "react";

export interface MfupConfigValue {
  /** Server base URL (may include a path prefix if the consumer mounted the
   * MFUP router under one, e.g. "https://host/api/uploads"). */
  serverUrl: string;
  /** Default target directory for uploads. */
  targetDir?: string;
  /** Default session meta (delivered to the server's authorize/map hooks). */
  meta?: unknown;
  chunkSize?: number;
  maxReconnectAttempts?: number | null;
}

const MfupContext = createContext<MfupConfigValue | null>(null);

export function MfupProvider(props: { config: MfupConfigValue; children?: ReactNode }) {
  return createElement(MfupContext.Provider, { value: props.config }, props.children);
}

/** The nearest MfupProvider config, or null when none is mounted. */
export function useMfupConfig(): MfupConfigValue | null {
  return useContext(MfupContext);
}

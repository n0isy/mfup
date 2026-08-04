// Drag-and-drop + file-picker glue. Wraps the browser-priority extraction
// from @mfup/client (getAsFileSystemHandle → webkitGetAsEntry → files),
// which must run synchronously inside the drop handler — it does.

import { useCallback, useRef, useState } from "react";
import type { ChangeEvent, DragEvent } from "react";
import { sourceFromDataTransfer, sourceFromInput, type UploadSource } from "@mfup/client";

export interface UseMfupDropzoneOptions {
  /** Called with a normalised source — usually feeds useMfupUpload().start. */
  onSource(source: UploadSource): void;
  disabled?: boolean;
}

export interface MfupDropzoneRootProps {
  onDragEnter(e: DragEvent<HTMLElement>): void;
  onDragOver(e: DragEvent<HTMLElement>): void;
  onDragLeave(e: DragEvent<HTMLElement>): void;
  onDrop(e: DragEvent<HTMLElement>): void;
}

export function useMfupDropzone(opts: UseMfupDropzoneOptions) {
  const [isDragActive, setDragActive] = useState(false);
  const optsRef = useRef(opts);
  optsRef.current = opts;
  // dragenter/dragleave bubble from every child — count depth or the
  // highlight flickers while moving across the dropzone's children.
  const depthRef = useRef(0);

  const getRootProps = useCallback((): MfupDropzoneRootProps => ({
    onDragEnter: (e) => {
      e.preventDefault();
      depthRef.current++;
      if (!optsRef.current.disabled) setDragActive(true);
    },
    onDragOver: (e) => {
      e.preventDefault();
    },
    onDragLeave: () => {
      depthRef.current = Math.max(0, depthRef.current - 1);
      if (depthRef.current === 0) setDragActive(false);
    },
    onDrop: (e) => {
      e.preventDefault();
      depthRef.current = 0;
      setDragActive(false);
      if (optsRef.current.disabled) return;
      const src = e.dataTransfer ? sourceFromDataTransfer(e.dataTransfer) : null;
      if (src) optsRef.current.onSource(src);
    },
  }), []);

  const getInputProps = useCallback((o?: { directory?: boolean }) => ({
    type: "file" as const,
    multiple: true,
    // webkitdirectory turns the picker into a folder picker (Chromium/WebKit;
    // Firefox supports it behind the same attribute).
    ...(o?.directory ? ({ webkitdirectory: "" } as Record<string, string>) : null),
    onChange: (e: ChangeEvent<HTMLInputElement>) => {
      if (optsRef.current.disabled) return;
      const src = sourceFromInput(e.currentTarget);
      // sourceFromInput copies out of the live FileList, so resetting the
      // input (to allow re-picking the same folder) is safe.
      e.currentTarget.value = "";
      if (src) optsRef.current.onSource(src);
    },
  }), []);

  return { isDragActive, getRootProps, getInputProps };
}

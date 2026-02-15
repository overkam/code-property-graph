import { useState, useCallback, useRef } from 'react';
import * as api from '@/api';

function isAbortError(e: unknown): boolean {
  return e instanceof Error && e.name === 'AbortError';
}

export function useSourceLoader() {
  const [sourceFile, setSourceFile] = useState<string | null>(null);
  const [sourceContent, setSourceContent] = useState<string | null>(null);
  const [sourceLoadFailed, setSourceLoadFailed] = useState(false);
  const [highlightLine, setHighlightLine] = useState<number | null>(null);
  const [loadSourcePending, setLoadSourcePending] = useState(false);

  const abortRef = useRef<AbortController | null>(null);

  const loadSourceForFile = useCallback((file: string, line: number | null) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setSourceLoadFailed(false);
    setLoadSourcePending(true);
    setSourceFile(file);
    setHighlightLine(line);
    api
      .apiSource(file, { signal: controller.signal })
      .then((r) => {
        if (abortRef.current !== controller) return;
        setSourceContent(r.content);
        setSourceLoadFailed(false);
      })
      .catch((e) => {
        if (isAbortError(e)) return;
        if (abortRef.current !== controller) return;
        setSourceContent(null);
        setSourceLoadFailed(true);
      })
      .finally(() => {
        if (abortRef.current === controller) {
          abortRef.current = null;
          setLoadSourcePending(false);
        }
      });
  }, []);

  const clearSource = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setSourceFile(null);
    setSourceContent(null);
    setSourceLoadFailed(false);
    setHighlightLine(null);
    setLoadSourcePending(false);
  }, []);

  return {
    sourceFile,
    sourceContent,
    sourceLoadFailed,
    highlightLine,
    loadSourcePending,
    loadSourceForFile,
    clearSource,
    setSourceFile,
    setSourceContent,
    setSourceLoadFailed,
    setHighlightLine,
  };
}

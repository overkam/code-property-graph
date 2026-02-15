export const DEFAULT_TIMEOUT_MS = 30_000;

export async function checkResponse(res: Response): Promise<void> {
  if (!res.ok) {
    const text = await res.text();
    let message = `HTTP ${res.status}`;
    try {
      const j = JSON.parse(text);
      if (j.error) message = j.error;
      else if (j.message) message = j.message;
    } catch {
      if (text) message = text.slice(0, 200);
    }
    throw new Error(message);
  }
}

/** Fetch with optional timeout; aborts on timeout or when signal aborts. */
export async function fetchWithTimeout(
  url: string,
  options: { signal?: AbortSignal; timeoutMs?: number } = {},
): Promise<Response> {
  const { signal, timeoutMs = DEFAULT_TIMEOUT_MS } = options;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  if (signal) {
    signal.addEventListener('abort', () => controller.abort());
  }
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

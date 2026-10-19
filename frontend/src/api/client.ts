// =====================================================================================================
// The one place that talks HTTP to the backend.
//   - adds X-User-Id (the buyer you are shopping as; the backend has no login yet) and, for POSTs that change
//     something, an Idempotency-Key
//   - turns every error answer into an ApiError with the backend's code (SOLD_OUT, STOCK_BUSY...), so the pages
//     switch on the code and never parse English text
//   - retries ONCE, with the SAME Idempotency-Key, when nginx says an app instance died mid-request (502/504) or
//     the network dropped. The same key means the retry can never buy twice: the backend replays the first answer
//     (HLD.md 10). This is what loadtest/chaos.sh's RETRY=on does for k6.
//   - remembers which app instance answered (nginx's X-Upstream header), for the strip at the bottom of the page
// =====================================================================================================
import type { Problem } from './types';

/** An error answer. `code` is the backend's stable code; `status` 0 means "no answer at all". */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(problem: Problem) {
    super(problem.detail ?? problem.title ?? `HTTP ${problem.status}`);
    this.status = problem.status;
    this.code = problem.code ?? `HTTP_${problem.status}`;
  }
}

export interface Answer<T> {
  status: number;
  data: T;
}

interface Options {
  body?: unknown;
  buyer?: string;
  idempotencyKey?: string;
}

// ---- which instance answered (nginx's X-Upstream header) ----
type UpstreamListener = (upstream: string) => void;
const upstreamListeners = new Set<UpstreamListener>();

export function onUpstream(listener: UpstreamListener): () => void {
  upstreamListeners.add(listener);
  return () => upstreamListeners.delete(listener);
}

/** Sends one request; on a gateway error, sends it once more with the same Idempotency-Key. */
export async function api<T>(method: string, path: string, options: Options = {}): Promise<Answer<T>> {
  const first = await send(method, path, options);
  const lost = first === null || first.status === 502 || first.status === 504;
  const response = lost && options.idempotencyKey ? await send(method, path, options) : first;
  if (response === null) {
    throw new ApiError({ status: 0, code: 'NETWORK', detail: 'The server could not be reached. Is it running?' });
  }
  const upstream = response.headers.get('X-Upstream');
  upstreamListeners.forEach((listener) => listener(upstream ?? 'direct (no nginx in front)'));

  const text = await response.text();
  const json = text ? safeJson(text) : null;
  if (!response.ok) {
    const problem = (json ?? {}) as Problem;
    throw new ApiError({ ...problem, status: response.status });
  }
  return { status: response.status, data: json as T };
}

async function send(method: string, path: string, options: Options): Promise<Response | null> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (options.body !== undefined) headers['Content-Type'] = 'application/json';
  if (options.buyer) headers['X-User-Id'] = options.buyer;
  if (options.idempotencyKey) headers['Idempotency-Key'] = options.idempotencyKey;
  try {
    return await fetch(path, {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
  } catch {
    return null;   // no answer: the server is down, or the connection was cut
  }
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/** A fresh Idempotency-Key. crypto.randomUUID only exists on https and localhost, so there is a fallback. */
export function newKey(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

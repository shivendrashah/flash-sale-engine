// =====================================================================================================
// Hooks shared by the pages.
//   useNow       the current time, re-read every `everyMs`: drives the countdowns
//   usePolling   calls a function now and then every `everyMs`, while the page is open. The pages poll once a
//                second: the sale page's stock, the line, the demo console's numbers. (A push channel such as
//                server-sent events would save requests; one request a second per open page is fine here, and
//                the sale page is served from the Redis cache.)
// =====================================================================================================
import { useEffect, useRef, useState } from 'react';

export function useNow(everyMs = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), everyMs);
    return () => clearInterval(timer);
  }, [everyMs]);
  return now;
}

/**
 * Runs `task` at once and then every `everyMs` milliseconds, never two at a time (a slow answer delays the next
 * call instead of piling calls up). `null` for everyMs stops it.
 */
export function usePolling(task: () => Promise<void>, everyMs: number | null, deps: unknown[] = []): void {
  // A ref always holds the newest `task`, so the timer below doesn't restart on every render
  const latest = useRef(task);
  latest.current = task;

  useEffect(() => {
    if (everyMs === null) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const tick = async () => {
      try {
        await latest.current();
      } catch {
        // the page shows its own error state; keep polling
      }
      if (!stopped) timer = setTimeout(tick, everyMs);
    };
    tick();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [everyMs, ...deps]);
}

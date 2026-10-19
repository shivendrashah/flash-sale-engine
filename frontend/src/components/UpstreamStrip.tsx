// The strip at the bottom of every page: which app instance answered the last request. nginx adds the
// X-Upstream header (nginx/templates/default.conf.template); watching it change between two addresses shows the
// round robin across app1 and app2 (HLD.md 9.13). Without nginx (npm run dev against one app), it says so.
import { useEffect, useState } from 'react';
import { onUpstream } from '../api/client';
import { useBuyer } from '../lib/buyer';

export function UpstreamStrip() {
  const { buyer } = useBuyer();
  const [upstream, setUpstream] = useState<string | null>(null);
  const [answers, setAnswers] = useState(0);
  useEffect(
    () =>
      onUpstream((value) => {
        setUpstream(value);
        setAnswers((n) => n + 1);
      }),
    [],
  );
  return (
    <div className="upstream">
      <span>
        Shopping as <code>{buyer}</code>
      </span>
      <span>
        Last answer from <code>{upstream ?? 'nobody yet'}</code> ({answers} answers)
      </span>
    </div>
  );
}

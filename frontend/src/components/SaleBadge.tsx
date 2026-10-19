// The little status label on a sale: LIVE, STARTS IN 0:42, or ENDED. Uses the browser's clock.
import type { Sale } from '../api/types';
import { countdown } from '../lib/format';

export type Phase = 'upcoming' | 'live' | 'ended';

export function salePhase(sale: Sale, now: number): Phase {
  if (now < Date.parse(sale.startsAt)) return 'upcoming';
  if (now >= Date.parse(sale.endsAt)) return 'ended';
  return 'live';
}

export function SaleBadge({ sale, now }: { sale: Sale; now: number }) {
  const phase = salePhase(sale, now);
  if (phase === 'upcoming') {
    return <span className="badge soon">Starts in {countdown(Date.parse(sale.startsAt) - now)}</span>;
  }
  if (phase === 'ended') return <span className="badge ended">Ended</span>;
  return <span className="badge live">Live</span>;
}

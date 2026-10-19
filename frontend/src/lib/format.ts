// Small helpers that turn numbers and times into text for the pages.

const rupees = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 });

export function money(amount: number): string {
  return rupees.format(amount);
}

/** "1:05", "12:00", "1h 05m", "2d 3h": a countdown that stays short. */
export function countdown(ms: number): string {
  const total = Math.max(0, Math.ceil(ms / 1000));
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`;
  return `${minutes}:${String(seconds).padStart(2, '0')}`;
}

export function percentOff(regular: number, sale: number): number {
  return regular > 0 ? Math.round((1 - sale / regular) * 100) : 0;
}

// =====================================================================================================
// The checkout panel: one buyer's way from "Buy now" to an order. Each step is one value of `step`:
//
//   choose ──Buy now──> (waiting room?) queue ──admitted──> holding ──201──> held ──Pay──> paying ──200──> done
//                                                                            ^   |                |
//                                                                            |   Cancel           202: the bank is
//                                                                            |   (stock back)     slow -> pending
//                                                                            └── card declined <──┘   -> done / held
//
//   queue    POST /api/sales/{id}/queue, then GET every second until ADMITTED
//   holding  POST /api/holds with a new Idempotency-Key
//   paying   POST /api/holds/{id}/pay with a new Idempotency-Key
//   pending  202 Accepted: GET /api/holds/{id} every 1.5 s until it is CONFIRMED or back to HELD
//                     (the payment recovery job settles it)
//
// Every error answer has a code, and errorNotice() turns each code into a sentence for a person.
// The hold id is kept in localStorage, so a reload in the middle of paying comes back to the same hold.
// =====================================================================================================
import { useEffect, useState } from 'react';
import { api, ApiError, newKey } from '../api/client';
import type { Hold, Payment, QueueStatus, Sale } from '../api/types';
import { useBuyer } from '../lib/buyer';
import { countdown, money } from '../lib/format';
import { useNow, usePolling } from '../lib/hooks';
import type { Phase } from './SaleBadge';

type Step =
  | { kind: 'choose' }
  | { kind: 'queue'; status?: QueueStatus; firstPosition?: number }
  | { kind: 'holding' }
  | { kind: 'held'; hold: Hold }
  | { kind: 'paying'; hold: Hold }
  | { kind: 'pending'; hold: Hold }
  | { kind: 'done'; hold: Hold; orderId?: number | null };

interface Notice {
  tone: 'ok' | 'warn' | 'bad' | 'info';
  text: string;
}

/** The fake payment provider's test cards (payment/gateway/FakePaymentGateway.java). */
const TEST_CARDS = [
  { token: 'tok_success', name: 'Card that works', hint: 'The payment goes through.' },
  { token: 'tok_declined', name: 'Card that is declined', hint: 'The bank says no. Your reservation stays.' },
  {
    token: 'tok_timeout',
    name: 'Slow bank',
    hint: 'The bank charges you but answers too late. The recovery job finds the charge and confirms the order.',
  },
  { token: 'tok_unavailable', name: 'Bank is down', hint: 'Nothing is charged. Try again later.' },
];

interface Props {
  sale: Sale;
  phase: Phase;
  quantities: Record<number, number>;
  onLock: (locked: boolean) => void;
}

export function Checkout({ sale, phase, quantities, onLock }: Props) {
  const { buyer } = useBuyer();
  const now = useNow();
  const [step, setStep] = useState<Step>({ kind: 'choose' });
  const [notice, setNotice] = useState<Notice | null>(null);
  const [card, setCard] = useState(TEST_CARDS[0].token);
  const savedHoldKey = `flashsale.hold.${buyer}.${sale.id}`;

  useEffect(() => onLock(step.kind !== 'choose'), [step.kind, onLock]);

  // ---- remember the hold across reloads ----
  const remember = (holdId: number | null) => {
    try {
      if (holdId === null) localStorage.removeItem(savedHoldKey);
      else localStorage.setItem(savedHoldKey, String(holdId));
    } catch {
      // storage blocked: a reload starts over, and the hold expires on its own
    }
  };

  // On open: if this buyer has a hold for this sale, continue from it
  useEffect(() => {
    let saved: string | null = null;
    try {
      saved = localStorage.getItem(savedHoldKey);
    } catch {
      return;
    }
    if (!saved) return;
    api<Hold>('GET', `/api/holds/${saved}`, { buyer })
      .then(({ data: hold }) => {
        if (hold.status === 'HELD') setStep({ kind: 'held', hold });
        else if (hold.status === 'PAYMENT_PENDING') setStep({ kind: 'pending', hold });
        else if (hold.status === 'CONFIRMED') setStep({ kind: 'done', hold });
        else remember(null);
      })
      .catch(() => remember(null));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ---- the waiting room ----
  const joinLine = async () => {
    setNotice(null);
    const { data } = await api<QueueStatus>('POST', `/api/sales/${sale.id}/queue`, { buyer });
    if (data.state === 'ADMITTED') return placeHold();
    setStep({ kind: 'queue', status: data, firstPosition: data.position });
  };

  usePolling(
    async () => {
      const { data } = await api<QueueStatus>('GET', `/api/sales/${sale.id}/queue`, { buyer });
      if (data.state === 'ADMITTED') return placeHold();
      if (data.state === 'NOT_QUEUED') return joinLine();   // the pass ran out: back in line
      setStep((s) => (s.kind === 'queue' ? { ...s, status: data } : s));
    },
    step.kind === 'queue' ? 1000 : null,
  );

  // ---- hold the stock ----
  const placeHold = async (): Promise<void> => {
    setStep({ kind: 'holding' });
    setNotice(null);
    const items = Object.entries(quantities)
      .filter(([, quantity]) => quantity > 0)
      .map(([saleItemId, quantity]) => ({ saleItemId: Number(saleItemId), quantity }));
    try {
      const { data: hold } = await api<Hold>('POST', '/api/holds', {
        buyer,
        idempotencyKey: newKey(),   // one key per click; the client's retry reuses it
        body: { items },
      });
      remember(hold.holdId);
      setStep({ kind: 'held', hold });
    } catch (e) {
      if (e instanceof ApiError && e.code === 'NOT_ADMITTED') return joinLine();
      setStep({ kind: 'choose' });
      setNotice(errorNotice(e));
    }
  };

  const start = async () => {
    try {
      if (sale.admitPerSecond) await joinLine();
      else await placeHold();
    } catch (e) {
      setStep({ kind: 'choose' });
      setNotice(errorNotice(e));
    }
  };

  // ---- pay ----
  const pay = async (hold: Hold) => {
    setStep({ kind: 'paying', hold });
    setNotice(null);
    try {
      const { status, data } = await api<Payment>('POST', `/api/holds/${hold.holdId}/pay`, {
        buyer,
        idempotencyKey: newKey(),   // a new attempt (e.g. another card) is a new payment request
        body: { provider: 'STRIPE', paymentToken: card },
      });
      if (status === 202 || data.paymentStatus === 'PENDING') {
        setStep({ kind: 'pending', hold });
      } else if (data.paymentStatus === 'SUCCEEDED') {
        setStep({ kind: 'done', hold, orderId: data.orderId });
      } else {
        setStep({ kind: 'held', hold });
        setNotice({ tone: 'bad', text: declineText(data.failureReason) });
      }
    } catch (e) {
      if (e instanceof ApiError && e.code === 'PAYMENT_IN_PROGRESS') return setStep({ kind: 'pending', hold });
      if (e instanceof ApiError && e.code === 'HOLD_EXPIRED') {
        remember(null);
        setStep({ kind: 'choose' });
      } else {
        setStep({ kind: 'held', hold });
      }
      setNotice(errorNotice(e));
    }
  };

  // ---- a payment whose answer is still unknown ----
  usePolling(
    async () => {
      if (step.kind !== 'pending') return;
      const { data: hold } = await api<Hold>('GET', `/api/holds/${step.hold.holdId}`, { buyer });
      if (hold.status === 'CONFIRMED') setStep({ kind: 'done', hold });
      else if (hold.status === 'HELD') {
        setStep({ kind: 'held', hold });
        setNotice({ tone: 'warn', text: 'The payment did not go through, and nothing was charged. Try again.' });
      } else if (hold.status === 'EXPIRED' || hold.status === 'CANCELLED') {
        remember(null);
        setStep({ kind: 'choose' });
        setNotice({ tone: 'bad', text: 'Your reservation ended before the payment was settled.' });
      }
    },
    step.kind === 'pending' ? 1500 : null,
  );

  const cancel = async (hold: Hold) => {
    try {
      await api<Hold>('DELETE', `/api/holds/${hold.holdId}`, { buyer });
      setNotice({ tone: 'info', text: 'Reservation cancelled. The units went back on sale.' });
    } catch (e) {
      setNotice(errorNotice(e));
    }
    remember(null);
    setStep({ kind: 'choose' });
  };

  // A reservation that runs out while the page is open
  const expired = step.kind === 'held' && now >= Date.parse(step.hold.expiresAt);
  useEffect(() => {
    if (!expired) return;
    remember(null);
    setStep({ kind: 'choose' });
    setNotice({ tone: 'bad', text: 'Your reservation ran out, and the units went back on sale.' });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expired]);

  // ---- what the panel shows ----
  const picked = Object.values(quantities).reduce((a, b) => a + b, 0);
  const total = sale.items.reduce((sum, i) => sum + i.salePrice * (quantities[i.saleItemId] ?? 0), 0);
  // every picked item shows 0 left: nothing to try for. (The page's stock is up to ~1 s old, and units come back
  // when someone cancels or a reservation runs out, so the button opens again on its own.)
  const soldOut = picked > 0 && sale.items.every((i) => !quantities[i.saleItemId] || i.availableStock === 0);
  const noticeBox = notice && <div className={`note ${notice.tone}`}>{notice.text}</div>;

  switch (step.kind) {
    case 'choose':
      return (
        <div className="card">
          <div className="step">Your order</div>
          <div className="big">{money(total)}</div>
          <div className="summary">
            <span>{picked} item{picked === 1 ? '' : 's'}</span>
            {sale.admitPerSecond && <span>You will join a short line first</span>}
          </div>
          {noticeBox}
          <button className="btn" disabled={phase !== 'live' || picked === 0 || soldOut} onClick={start}>
            {soldOut && phase !== 'ended'
              ? 'Sold out'
              : phase === 'upcoming'
              ? `Starts in ${countdown(Date.parse(sale.startsAt) - now)}`
              : phase === 'ended'
                ? 'This sale has ended'
                : picked === 0
                  ? 'Pick at least one item'
                  : 'Buy now'}
          </button>
          {phase === 'upcoming' && sale.admitPerSecond && (
            <button className="btn ghost" onClick={() => joinLine().catch((e) => setNotice(errorNotice(e)))}>
              Join the line now
            </button>
          )}
        </div>
      );

    case 'queue': {
      const position = step.status?.position ?? 0;
      const first = step.firstPosition ?? position;
      const moved = first > 0 ? ((first - position) / first) * 100 : 0;
      return (
        <div className="card">
          <div className="step">Waiting room</div>
          <div className="big">#{position || '…'}</div>
          <div>in line{step.status?.estimatedWaitSeconds !== undefined && <> · about {step.status.estimatedWaitSeconds} s</>}</div>
          <div className="progress">
            <i style={{ width: `${moved}%` }} />
          </div>
          <div className="note info">
            {phase === 'upcoming'
              ? 'The sale has not started. The line starts moving when it does. Keep this page open.'
              : `The shop lets in ${sale.admitPerSecond} buyers a second, first come first served. Keep this page open: your item is reserved the moment you are in.`}
          </div>
        </div>
      );
    }

    case 'holding':
      return (
        <div className="card">
          <div className="step">Reserving</div>
          <p>
            <span className="spinner" /> Reserving your items…
          </p>
        </div>
      );

    case 'held':
    case 'paying':
      return (
        <div className="card">
          <div className="step">Reserved for you</div>
          <div className="big countdown">{countdown(Date.parse(step.hold.expiresAt) - now)}</div>
          <div className="summary">
            <span>left to pay</span>
            <b>{money(step.hold.totalAmount)}</b>
          </div>
          {noticeBox}
          <div className="cards" style={{ marginTop: 12 }}>
            {TEST_CARDS.map((c) => (
              <label key={c.token}>
                <input type="radio" name="card" checked={card === c.token} onChange={() => setCard(c.token)} />
                <span>
                  {c.name}
                  <small>{c.hint}</small>
                </span>
              </label>
            ))}
          </div>
          <button className="btn" disabled={step.kind === 'paying'} onClick={() => pay(step.hold)}>
            {step.kind === 'paying' ? <><span className="spinner" /> Paying…</> : `Pay ${money(step.hold.totalAmount)}`}
          </button>
          <button className="btn ghost" disabled={step.kind === 'paying'} onClick={() => cancel(step.hold)}>
            Cancel and release the items
          </button>
        </div>
      );

    case 'pending':
      return (
        <div className="card">
          <div className="step">Checking with your bank</div>
          <p>
            <span className="spinner" /> Your bank has not answered yet.
          </p>
          <div className="note info">
            Your items stay reserved while we find out. The payment recovery job asks the bank again, so you are
            never charged twice and never lose a payment. This page updates on its own.
          </div>
        </div>
      );

    case 'done':
      return (
        <div className="card">
          <div className="step">Order confirmed</div>
          <div className="big">✓ Paid</div>
          <div className="summary">
            <span>{step.orderId ? `Order #${step.orderId}` : 'Your order'}</span>
            <b>{money(step.hold.totalAmount)}</b>
          </div>
          <div className="note ok">
            A confirmation is on its way. It goes through Kafka, so it can arrive a moment after this page.
          </div>
          <button
            className="btn ghost"
            onClick={() => {
              remember(null);
              setNotice(null);
              setStep({ kind: 'choose' });
            }}
          >
            Buy again
          </button>
        </div>
      );
  }
}

/** The backend's error code, as a sentence for a person. */
function errorNotice(e: unknown): Notice {
  if (!(e instanceof ApiError)) return { tone: 'bad', text: String(e) };
  switch (e.code) {
    case 'SOLD_OUT':
      return { tone: 'bad', text: 'Sold out. Every unit is reserved or sold. Units come back if someone does not pay, so try again in a minute.' };
    case 'STOCK_BUSY':
      return { tone: 'warn', text: 'So many people are buying right now that we could not reserve it in time. Try again.' };
    case 'PURCHASE_LIMIT_EXCEEDED':
      return { tone: 'warn', text: e.message };
    case 'RATE_LIMITED':
    case 'IP_RATE_LIMITED':
      return { tone: 'warn', text: 'Too many clicks. Wait a second and try again.' };
    case 'SALE_NOT_ACTIVE':
      return { tone: 'bad', text: 'This sale is not running right now.' };
    case 'HOLD_EXPIRED':
      return { tone: 'bad', text: 'Your reservation ran out, and the units went back on sale.' };
    case 'PAYMENT_PROVIDER_UNAVAILABLE':
      return { tone: 'warn', text: 'The payment provider is down. Nothing was charged. Try again in a few seconds.' };
    case 'NETWORK':
      return { tone: 'bad', text: e.message };
    default:
      return { tone: 'bad', text: `${e.message} (${e.code})` };
  }
}

function declineText(reason?: string | null): string {
  if (reason === 'card_declined') return 'Your card was declined. Nothing was charged. Try another card.';
  return `The payment failed (${reason ?? 'unknown reason'}). Nothing was charged.`;
}

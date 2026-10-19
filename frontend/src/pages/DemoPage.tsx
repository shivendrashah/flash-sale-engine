// =====================================================================================================
// The demo console: for showing the system to someone.
//   1. Create a sale (POST /api/admin/sales): stock, price, when it starts, a per-buyer limit, a waiting room.
//   2. Send a crowd of fake buyers at it (POST /api/admin/sales/{id}/crowd, demo/CrowdSimulator).
//   3. Watch it live, once a second (GET /api/admin/sales/{id}/stats): units left, held and sold, orders, orders
//      notified through Kafka, the line, and "oversold", which must stay 0 whatever the crowd does.
// Open the sale in another tab as a buyer and try to get a unit while the crowd buys.
// =====================================================================================================
import { useEffect, useState, type FormEvent } from 'react';
import { Link, useSearchParams } from 'react-router';
import { api, ApiError } from '../api/client';
import type { CreateSale, Sale, SaleStats } from '../api/types';
import { StockBar } from '../components/StockBar';
import { StockChart, type Point } from '../components/StockChart';
import { usePolling } from '../lib/hooks';

/** The crowd counters' labels, in demo/CrowdSimulator.COUNTERS order. */
const CROWD_LABELS: Record<string, string> = {
  sent: 'Sent',
  arrived: 'Arrived',
  held: 'Reserved',
  paid: 'Paid',
  cancelled: 'Changed mind',
  soldOut: 'Told "sold out"',
  busy: 'Told "busy"',
  notAdmitted: 'Gave up in line',
  failed: 'Failed',
  done: 'Finished',
};

const HISTORY = 180;   // points on the chart: the last 3 minutes

export function DemoPage() {
  const [params, setParams] = useSearchParams();
  const selected = params.get('sale') ? Number(params.get('sale')) : null;
  const [sales, setSales] = useState<Sale[]>([]);
  const [stats, setStats] = useState<SaleStats | null>(null);
  const [history, setHistory] = useState<Point[]>([]);
  const [error, setError] = useState<string | null>(null);

  const select = (id: number) => setParams({ sale: String(id) });

  usePolling(async () => {
    const list = (await api<Sale[]>('GET', '/api/sales')).data;
    setSales(list);
    if (selected === null && list.length > 0) select(list[0].id);
  }, 3000, [selected]);

  useEffect(() => {
    setStats(null);
    setHistory([]);
  }, [selected]);

  usePolling(
    async () => {
      if (selected === null) return;
      try {
        const s = (await api<SaleStats>('GET', `/api/admin/sales/${selected}/stats`)).data;
        setStats(s);
        setError(null);
        const sum = (f: (i: SaleStats['items'][number]) => number) => s.items.reduce((n, i) => n + f(i), 0);
        const point = { t: Date.now(), left: sum((i) => i.availableStock), held: sum((i) => i.held), sold: sum((i) => i.sold) };
        setHistory((h) => [...h, point].slice(-HISTORY));
      } catch (e) {
        setError(e instanceof ApiError ? `${e.message} (${e.code})` : String(e));
      }
    },
    selected === null ? null : 1000,
    [selected],
  );

  const sale = sales.find((s) => s.id === selected) ?? null;

  return (
    <>
      <h1>Demo console</h1>
      <p className="sub">Create a sale, send a crowd at it, and watch the stock. Oversold must stay at 0.</p>
      <div className="demo">
        <div>
          <CreateSaleForm onCreated={(s) => select(s.id)} />
          <CrowdForm saleId={selected} />
          <div className="card">
            <h2>What to watch</h2>
            <ul className="explain">
              <li>
                <b>Oversold</b> counts units sold beyond the stock. It stays at 0, however many buyers race.
              </li>
              <li>
                <b>Held</b> units are reserved and waiting for payment. A buyer who cancels sends them back to{' '}
                <b>Left</b>, and a later buyer gets them.
              </li>
              <li>
                <b>Notified</b> catches up with <b>Orders</b> a moment later: confirmations go through Kafka, so a
                slow mail server never slows down buying.
              </li>
              <li>With a waiting room, the line forms before the start and moves at the set rate.</li>
            </ul>
          </div>
        </div>

        <div>
          <div className="card">
            <div className="row" style={{ alignItems: 'center' }}>
              <select className="field" value={selected ?? ''} onChange={(e) => select(Number(e.target.value))}>
                {sales.length === 0 && <option value="">No sales yet: create one</option>}
                {sales.map((s) => (
                  <option key={s.id} value={s.id}>
                    #{s.id} {s.name}
                  </option>
                ))}
              </select>
              {sale && (
                <Link className="btn small ghost" style={{ flex: '0 0 auto' }} to={`/sale/${sale.id}`} target="_blank">
                  Open as a buyer ↗
                </Link>
              )}
            </div>
          </div>
          {error && <div className="note bad">{error}</div>}
          {stats && <StatsBoard stats={stats} history={history} />}
        </div>
      </div>
    </>
  );
}

function StatsBoard({ stats, history }: { stats: SaleStats; history: Point[] }) {
  const sum = (f: (i: SaleStats['items'][number]) => number) => stats.items.reduce((n, i) => n + f(i), 0);
  const total = sum((i) => i.totalStock);
  const oversold = sum((i) => i.oversold);
  const mismatch = stats.items.some((i) => i.mismatch);
  const crowd = stats.crowd;
  const crowdDone = crowd.sent ? Math.round(((crowd.done ?? 0) / crowd.sent) * 100) : 0;

  return (
    <>
      <div className="card">
        <div className="tiles">
          <Tile label="Left" value={sum((i) => i.availableStock)} />
          <Tile label="Held" value={sum((i) => i.held)} />
          <Tile label="Sold" value={sum((i) => i.sold)} />
          <Tile label="Oversold" value={oversold} tone={oversold === 0 ? 'good' : 'alarm'} />
          <Tile label="Orders" value={stats.orders} />
          <Tile label="Notified" value={stats.notified} />
          <Tile label="In line" value={stats.inQueue < 0 ? '?' : stats.inQueue} />
        </div>
        <StockChart points={history} total={total} />
        <div className="legend">
          <span><i style={{ background: 'var(--sold)' }} />Sold</span>
          <span><i style={{ background: 'var(--held)' }} />Held</span>
          <span><i style={{ background: 'var(--left)' }} />Left</span>
          <span style={{ marginLeft: 'auto' }}>last 3 minutes</span>
        </div>
        {mismatch && (
          <div className="note bad">
            The stock counter and the holds disagree. A lost update happened (the NAIVE strategy does this).
          </div>
        )}
      </div>

      {stats.items.length > 1 && (
        <div className="card">
          <h2>Per item</h2>
          {stats.items.map((i) => (
            <div key={i.saleItemId} style={{ marginBottom: 12 }}>
              <div className="stock-text" style={{ marginTop: 0 }}>
                <b>{i.productName}</b>
                <span>{i.availableStock} left · {i.held} held · {i.sold} sold</span>
              </div>
              <StockBar total={i.totalStock} left={i.availableStock} held={i.held} sold={i.sold} />
            </div>
          ))}
        </div>
      )}

      {crowd.sent > 0 && (
        <div className="card">
          <h2>The crowd</h2>
          <div className="stock-text" style={{ marginTop: 0 }}>
            <span>{crowd.done ?? 0} of {crowd.sent} buyers finished</span>
            <b>{crowdDone}%</b>
          </div>
          <div className="progress">
            <i style={{ width: `${crowdDone}%` }} />
          </div>
          <div className="crowd-counts">
            {Object.entries(CROWD_LABELS)
              .filter(([key]) => key !== 'sent' && key !== 'done')
              .map(([key, label]) => (
                <div key={key}>
                  <span>{label}</span>
                  <b>{crowd[key] ?? 0}</b>
                </div>
              ))}
          </div>
        </div>
      )}
    </>
  );
}

function Tile({ label, value, tone }: { label: string; value: number | string; tone?: 'good' | 'alarm' }) {
  return (
    <div className={`tile ${tone ?? ''}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function CreateSaleForm({ onCreated }: { onCreated: (sale: Sale) => void }) {
  const [form, setForm] = useState({
    name: 'Diwali iPhone drop',
    product: 'iPhone 17',
    regularPrice: 79999,
    salePrice: 59999,
    stock: 100,
    startsIn: 30,
    minutes: 15,
    perUserLimit: 2,
    admitPerSecond: 0,
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const set = (key: keyof typeof form) => (e: { target: { value: string } }) =>
    setForm((f) => ({ ...f, [key]: key === 'name' || key === 'product' ? e.target.value : Number(e.target.value) }));

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      // the times are worked out at the click, not when the form was last edited
      const starts = Date.now() + form.startsIn * 1000;
      const body: CreateSale = {
        name: form.name,
        startsAt: new Date(starts).toISOString(),
        endsAt: new Date(starts + form.minutes * 60_000).toISOString(),
        perUserLimit: form.perUserLimit > 0 ? form.perUserLimit : undefined,
        admitPerSecond: form.admitPerSecond > 0 ? form.admitPerSecond : undefined,
        items: [{ productName: form.product, regularPrice: form.regularPrice, salePrice: form.salePrice, stock: form.stock }],
      };
      onCreated((await api<Sale>('POST', '/api/admin/sales', { body })).data);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="card form" onSubmit={submit}>
      <h2>1. Create a sale</h2>
      <label>Sale name</label>
      <input value={form.name} onChange={set('name')} required maxLength={200} />
      <label>Product</label>
      <input value={form.product} onChange={set('product')} required maxLength={200} />
      <div className="row">
        <div>
          <label>Regular price</label>
          <input type="number" min={0} value={form.regularPrice} onChange={set('regularPrice')} />
        </div>
        <div>
          <label>Sale price</label>
          <input type="number" min={0} value={form.salePrice} onChange={set('salePrice')} />
        </div>
      </div>
      <div className="row">
        <div>
          <label>Stock</label>
          <input type="number" min={1} max={1000000} value={form.stock} onChange={set('stock')} />
        </div>
        <div>
          <label>Limit per buyer</label>
          <select value={form.perUserLimit} onChange={set('perUserLimit')}>
            <option value={0}>None</option>
            {[1, 2, 3, 5].map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
      </div>
      <div className="row">
        <div>
          <label>Starts in</label>
          <select value={form.startsIn} onChange={set('startsIn')}>
            <option value={0}>Now</option>
            <option value={15}>15 seconds</option>
            <option value={30}>30 seconds</option>
            <option value={60}>1 minute</option>
          </select>
        </div>
        <div>
          <label>Runs for</label>
          <select value={form.minutes} onChange={set('minutes')}>
            <option value={5}>5 minutes</option>
            <option value={15}>15 minutes</option>
            <option value={60}>1 hour</option>
          </select>
        </div>
      </div>
      <label>Waiting room</label>
      <select value={form.admitPerSecond} onChange={set('admitPerSecond')}>
        <option value={0}>Off: everyone goes straight to the stock</option>
        <option value={5}>Let in 5 buyers a second</option>
        <option value={20}>Let in 20 buyers a second</option>
        <option value={100}>Let in 100 buyers a second</option>
      </select>
      {error && <div className="note bad">{error}</div>}
      <button className="btn" disabled={busy}>{busy ? 'Creating…' : 'Create the sale'}</button>
    </form>
  );
}

function CrowdForm({ saleId }: { saleId: number | null }) {
  const [buyers, setBuyers] = useState(500);
  const [seconds, setSeconds] = useState(5);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ tone: string; text: string } | null>(null);

  const send = async (e: FormEvent) => {
    e.preventDefault();
    if (saleId === null) return;
    setBusy(true);
    setNotice(null);
    try {
      await api('POST', `/api/admin/sales/${saleId}/crowd`, { body: { buyers, seconds } });
      setNotice({ tone: 'ok', text: `${buyers} buyers are on their way. Watch the numbers on the right.` });
    } catch (err) {
      setNotice({ tone: 'bad', text: err instanceof ApiError ? err.message : String(err) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="card form" onSubmit={send}>
      <h2>2. Send a crowd</h2>
      <div className="row">
        <div>
          <label>Buyers</label>
          <select value={buyers} onChange={(e) => setBuyers(Number(e.target.value))}>
            {[50, 200, 500, 1000, 2000].map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
        <div>
          <label>Arriving over</label>
          <select value={seconds} onChange={(e) => setSeconds(Number(e.target.value))}>
            <option value={0}>All at once</option>
            <option value={5}>5 seconds</option>
            <option value={15}>15 seconds</option>
            <option value={30}>30 seconds</option>
          </select>
        </div>
      </div>
      <p className="explain" style={{ padding: 0, marginTop: 10 }}>
        Each fake buyer reserves 1 unit, thinks for up to 3 seconds and pays. Every 7th one changes their mind and
        cancels. If the sale has not started, they wait for it.
      </p>
      {notice && <div className={`note ${notice.tone}`}>{notice.text}</div>}
      <button className="btn" disabled={busy || saleId === null}>{busy ? 'Sending…' : 'Send the crowd'}</button>
    </form>
  );
}

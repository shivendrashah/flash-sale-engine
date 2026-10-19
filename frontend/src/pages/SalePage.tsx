// =====================================================================================================
// One sale: its items with live stock (refreshed every second), a quantity picker per item, and the
// checkout panel (components/Checkout.tsx) that walks the buyer through line -> hold -> pay.
// Calls GET /api/sales/{saleId}: served from the Redis copy of the sale page, so polling it is cheap.
// =====================================================================================================
import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { api, ApiError } from '../api/client';
import type { Sale } from '../api/types';
import { Checkout } from '../components/Checkout';
import { SaleBadge, salePhase } from '../components/SaleBadge';
import { StockBar } from '../components/StockBar';
import { useBuyer } from '../lib/buyer';
import { countdown, money, percentOff } from '../lib/format';
import { useNow, usePolling } from '../lib/hooks';

/** Most units one buyer can pick per item, when the sale sets no limit. */
const MAX_PICK = 5;

export function SalePage() {
  const saleId = Number(useParams().saleId);
  const { buyer } = useBuyer();
  const now = useNow();
  const [sale, setSale] = useState<Sale | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [quantities, setQuantities] = useState<Record<number, number>>({});
  const [locked, setLocked] = useState(false);   // the checkout is past "choose": the picks can't change

  usePolling(
    async () => {
      try {
        const loaded = (await api<Sale>('GET', `/api/sales/${saleId}`)).data;
        setSale(loaded);
        setError(null);
        // first load: 1 of the first item, so "Buy now" works with one click
        setQuantities((q) => (Object.keys(q).length ? q : { [loaded.items[0].saleItemId]: 1 }));
      } catch (e) {
        setError(e instanceof ApiError ? e.message : String(e));
      }
    },
    1000,
    [saleId],
  );

  if (!sale) {
    return error ? <div className="note bad">{error}</div> : <p className="empty">Loading…</p>;
  }

  const phase = salePhase(sale, now);
  const limit = sale.perUserLimit ?? MAX_PICK;
  const picked = Object.values(quantities).reduce((a, b) => a + b, 0);
  const setQty = (id: number, qty: number) => setQuantities((q) => ({ ...q, [id]: Math.max(0, qty) }));

  return (
    <>
      <Link to="/" className="back">
        ← All sales
      </Link>
      <div className="two">
        <section>
          <h1>{sale.name}</h1>
          <div className="meta">
            <SaleBadge sale={sale} now={now} />
            {phase === 'live' && (
              <span>
                Ends in <span className="countdown">{countdown(Date.parse(sale.endsAt) - now)}</span>
              </span>
            )}
            {sale.perUserLimit && <span>Limit {sale.perUserLimit} per buyer</span>}
            {sale.admitPerSecond && <span className="badge queue">Waiting room · {sale.admitPerSecond} buyers/s</span>}
          </div>
          {error && <div className="note warn">Could not refresh the stock: {error}</div>}
          <div className="card">
            {sale.items.map((item) => {
              const qty = quantities[item.saleItemId] ?? 0;
              const max = Math.min(MAX_PICK, item.availableStock);
              return (
                <div className="item" key={item.saleItemId}>
                  <div>
                    <div className="name">{item.productName}</div>
                    <span className="price">{money(item.salePrice)}</span>
                    <span className="was">{money(item.regularPrice)}</span>
                    <span className="off">{percentOff(item.regularPrice, item.salePrice)}% off</span>
                  </div>
                  <div className="qty" aria-label={`Quantity of ${item.productName}`}>
                    <button disabled={locked || qty <= 0} onClick={() => setQty(item.saleItemId, qty - 1)}>
                      −
                    </button>
                    <span>{qty}</span>
                    <button
                      disabled={locked || qty >= max || picked >= limit}
                      onClick={() => setQty(item.saleItemId, qty + 1)}
                    >
                      +
                    </button>
                  </div>
                  <div className="stock">
                    <StockBar total={item.totalStock} left={item.availableStock} />
                    <div className="stock-text">
                      <span>
                        <b>{item.availableStock}</b> of {item.totalStock} left
                      </span>
                      {item.availableStock === 0 ? <b>Sold out</b> : item.availableStock <= 5 && <b>Almost gone</b>}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </section>
        <aside className="panel">
          {/* key: a new buyer name or a new sale starts a fresh checkout (all its state is reset) */}
          <Checkout key={`${buyer}:${sale.id}`} sale={sale} phase={phase} quantities={quantities} onLock={setLocked} />
        </aside>
      </div>
    </>
  );
}

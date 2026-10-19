// =====================================================================================================
// The shop's front page: the newest sales as cards, refreshed every 2 seconds.
// Calls GET /api/sales (SaleController.listSales).
// =====================================================================================================
import { useState } from 'react';
import { Link } from 'react-router';
import { api, ApiError } from '../api/client';
import type { Sale } from '../api/types';
import { SaleBadge } from '../components/SaleBadge';
import { StockBar } from '../components/StockBar';
import { money } from '../lib/format';
import { useNow, usePolling } from '../lib/hooks';

export function ShopPage() {
  const now = useNow();
  const [sales, setSales] = useState<Sale[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  usePolling(async () => {
    try {
      setSales((await api<Sale[]>('GET', '/api/sales')).data);
      setError(null);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  }, 2000);

  return (
    <>
      <h1>Flash sales</h1>
      <p className="sub">Limited stock at a big discount. When it is gone, it is gone.</p>
      {error && <div className="note bad">{error}</div>}
      {sales === null && !error && <p className="empty">Loading…</p>}
      {sales?.length === 0 && (
        <div className="card empty">
          No sales yet. Create one in the <Link to="/demo">demo console</Link>.
        </div>
      )}
      <div className="grid">
        {sales?.map((sale) => {
          const total = sale.items.reduce((n, i) => n + i.totalStock, 0);
          const left = sale.items.reduce((n, i) => n + i.availableStock, 0);
          const cheapest = Math.min(...sale.items.map((i) => i.salePrice));
          return (
            <Link key={sale.id} to={`/sale/${sale.id}`} className="card sale-card">
              <div className="meta" style={{ margin: 0 }}>
                <SaleBadge sale={sale} now={now} />
                {sale.admitPerSecond && <span className="badge queue">Waiting room</span>}
              </div>
              <h3>{sale.name}</h3>
              <div className="items">{sale.items.map((i) => i.productName).join(' · ')}</div>
              <div className="price" style={{ fontSize: 18, marginBottom: 10 }}>
                from {money(cheapest)}
              </div>
              <StockBar total={total} left={left} />
              <div className="stock-text">
                <span>
                  <b>{left}</b> of {total} left
                </span>
                {left === 0 && <b>Sold out</b>}
              </div>
            </Link>
          );
        })}
      </div>
    </>
  );
}

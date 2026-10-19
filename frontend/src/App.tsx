// =====================================================================================================
// The frame of every page, and which page each URL shows.
//   /              ShopPage: the newest sales                         GET /api/sales
//   /sale/:saleId  SalePage: one sale, its stock, and the checkout
//   /demo          DemoPage: create a sale, send a crowd, watch it
// React Router reads the URL in the browser: changing pages does not reload the app. nginx answers every
// unknown path with index.html, so a reload on /sale/3 still works (nginx/templates/default.conf.template).
// =====================================================================================================
import { useState } from 'react';
import { BrowserRouter, Link, NavLink, Route, Routes } from 'react-router';
import { BuyerProvider, useBuyer } from './lib/buyer';
import { UpstreamStrip } from './components/UpstreamStrip';
import { ShopPage } from './pages/ShopPage';
import { SalePage } from './pages/SalePage';
import { DemoPage } from './pages/DemoPage';

export function App() {
  return (
    <BuyerProvider>
      <BrowserRouter>
        <TopBar />
        <main>
          <Routes>
            <Route path="/" element={<ShopPage />} />
            <Route path="/sale/:saleId" element={<SalePage />} />
            <Route path="/demo" element={<DemoPage />} />
            <Route path="*" element={<p className="empty">This page does not exist. <Link to="/">Go to the shop</Link></p>} />
          </Routes>
        </main>
        <UpstreamStrip />
      </BrowserRouter>
    </BuyerProvider>
  );
}

function TopBar() {
  const { buyer, setBuyer } = useBuyer();
  const [draft, setDraft] = useState(buyer);
  return (
    <header className="topbar">
      <Link to="/" className="logo">
        Flash<span>Sale</span>
      </Link>
      <nav className="nav">
        <NavLink to="/" end>
          Shop
        </NavLink>
        <NavLink to="/demo">Demo console</NavLink>
      </nav>
      <form
        className="who"
        onSubmit={(e) => {
          e.preventDefault();
          setBuyer(draft);
        }}
      >
        <label htmlFor="buyer">Buying as</label>
        <input id="buyer" value={draft} onChange={(e) => setDraft(e.target.value)} onBlur={() => setBuyer(draft)} maxLength={64} />
      </form>
    </header>
  );
}

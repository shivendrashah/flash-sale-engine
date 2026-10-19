// A bar that shows an item's stock. The shop shows only what is left (green). The demo console also splits
// the rest into held (waiting for payment, amber) and sold (grey), from SaleStats.
interface Props {
  total: number;
  left: number;
  held?: number;
  sold?: number;
}

export function StockBar({ total, left, held, sold }: Props) {
  const pct = (n: number) => `${total > 0 ? (n / total) * 100 : 0}%`;
  const detailed = held !== undefined && sold !== undefined;
  return (
    <div className="bar" role="img" aria-label={`${left} of ${total} left`}>
      {detailed && <i className="sold" style={{ width: pct(sold) }} />}
      {detailed && <i className="held" style={{ width: pct(held) }} />}
      <i className="left" style={{ width: pct(left), marginLeft: detailed ? 0 : 'auto' }} />
    </div>
  );
}

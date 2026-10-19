// The demo console's chart: the sale's units over time, stacked: sold (grey), held (amber), left (green).
// A plain <canvas>, drawn again whenever a new point arrives (once a second). Colors come from the CSS variables,
// so the chart follows light and dark mode.
import { useEffect, useRef } from 'react';

export interface Point {
  t: number;
  left: number;
  held: number;
  sold: number;
}

export function StockChart({ points, total }: { points: Point[]; total: number }) {
  const canvas = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const el = canvas.current;
    if (!el) return;
    const ratio = window.devicePixelRatio || 1;
    const width = el.clientWidth;
    const height = el.clientHeight;
    el.width = width * ratio;
    el.height = height * ratio;
    const g = el.getContext('2d');
    if (!g) return;
    g.scale(ratio, ratio);
    g.clearRect(0, 0, width, height);

    const css = getComputedStyle(document.documentElement);
    const color = (name: string) => css.getPropertyValue(name).trim();
    g.strokeStyle = color('--line');
    g.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = Math.round((height * i) / 4) + 0.5;
      g.beginPath();
      g.moveTo(0, y);
      g.lineTo(width, y);
      g.stroke();
    }
    if (points.length < 2 || total <= 0) return;

    const x = (i: number) => (i / (points.length - 1)) * width;
    const y = (units: number) => height - (units / total) * height;
    // three stacked bands: [from, to] per point
    const bands: [string, (p: Point) => number, (p: Point) => number][] = [
      ['--sold', () => 0, (p) => p.sold],
      ['--held', (p) => p.sold, (p) => p.sold + p.held],
      ['--left', (p) => p.sold + p.held, (p) => p.sold + p.held + p.left],
    ];
    for (const [name, from, to] of bands) {
      g.beginPath();
      points.forEach((p, i) => (i === 0 ? g.moveTo(x(i), y(to(p))) : g.lineTo(x(i), y(to(p)))));
      for (let i = points.length - 1; i >= 0; i--) g.lineTo(x(i), y(from(points[i])));
      g.closePath();
      g.fillStyle = color(name);
      g.globalAlpha = 0.85;
      g.fill();
    }
    g.globalAlpha = 1;
  }, [points, total]);

  return <canvas ref={canvas} aria-label="Units sold, held and left over time" />;
}

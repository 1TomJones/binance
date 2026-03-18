import React, { useMemo } from 'react';

export function TickChart({ trades }) {
  const points = useMemo(() => {
    const series = trades.slice(-250);
    if (series.length < 2) return '';
    const prices = series.map((t) => t.price);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const range = max - min || 1;

    return series
      .map((t, i) => {
        const x = (i / (series.length - 1)) * 100;
        const y = 100 - ((t.price - min) / range) * 100;
        return `${x},${y}`;
      })
      .join(' ');
  }, [trades]);

  return (
    <div className="chart-wrap">
      <div className="chart-title">BTCUSDT · TRADE-DRIVEN TICK LINE</div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="tick-chart">
        {[20, 40, 60, 80].map((y) => (
          <line key={y} x1="0" y1={y} x2="100" y2={y} className="grid" />
        ))}
        <polyline points={points} className="tick-line" />
      </svg>
    </div>
  );
}

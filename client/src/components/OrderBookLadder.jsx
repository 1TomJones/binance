import React, { memo, useMemo, useState } from 'react';
import { UI_LIMITS } from '../constants/uiPerformance.js';

function fmtPrice(v) {
  return Number(v).toFixed(2);
}

function fmtQty(v) {
  return Number(v).toFixed(4);
}

function fmtNotional(v) {
  return Number(v).toFixed(2);
}

function buildAggregatedBands(depth) {
  const bestAsk = depth?.bestAsk?.price;
  const bestBid = depth?.bestBid?.price;
  if (!Number.isFinite(bestAsk) || !Number.isFinite(bestBid)) {
    return { asks: [], bids: [] };
  }

  const bandsPerSide = UI_LIMITS.orderBookLevelsPerSide;
  const askStart = Math.floor(bestAsk);
  const bidStart = Math.floor(bestBid);

  const askMap = new Map();
  (depth?.asks || []).forEach((level) => {
    const band = Math.floor(level.price);
    askMap.set(band, (askMap.get(band) || 0) + Number(level.quantity || 0));
  });

  const bidMap = new Map();
  (depth?.bids || []).forEach((level) => {
    const band = Math.floor(level.price);
    bidMap.set(band, (bidMap.get(band) || 0) + Number(level.quantity || 0));
  });

  const asks = Array.from({ length: bandsPerSide }, (_, idx) => {
    const price = askStart + idx;
    return { price, quantity: askMap.get(price) || 0 };
  });

  const bids = Array.from({ length: bandsPerSide }, (_, idx) => {
    const price = bidStart - idx;
    return { price, quantity: bidMap.get(price) || 0 };
  });

  return { asks, bids };
}

const OrderBookRow = memo(function OrderBookRow({ side, level, maxSize }) {
  const quantity = Number(level.quantity || 0);
  return (
    <div className={`book-row ${side}`}>
      <span className="depth-bg" style={{ width: `${(quantity / maxSize) * 100}%` }} />
      <span className="price">{fmtPrice(level.price)}</span>
      <span>{fmtQty(quantity)}</span>
      <span>{fmtNotional(level.price * quantity)}</span>
    </div>
  );
}, (prev, next) => prev.side === next.side
  && prev.maxSize === next.maxSize
  && prev.level.price === next.level.price
  && prev.level.quantity === next.level.quantity);

function summarizeDisplay(display) {
  let maxSize = 1;
  let totalVisibleAsks = 0;
  let totalVisibleBids = 0;

  display.asks.forEach((level) => {
    const qty = Number(level.quantity || 0);
    totalVisibleAsks += qty;
    if (qty > maxSize) maxSize = qty;
  });

  display.bids.forEach((level) => {
    const qty = Number(level.quantity || 0);
    totalVisibleBids += qty;
    if (qty > maxSize) maxSize = qty;
  });

  return { maxSize, totalVisibleAsks, totalVisibleBids };
}

function OrderBookLadderComponent({ depth }) {
  const [mode, setMode] = useState('raw');
  const asks = depth?.asks || [];
  const bids = depth?.bids || [];

  const display = useMemo(() => {
    if (mode === 'raw') {
      return {
        asks: asks.slice(0, UI_LIMITS.orderBookLevelsPerSide),
        bids: bids.slice(0, UI_LIMITS.orderBookLevelsPerSide)
      };
    }
    return buildAggregatedBands(depth);
  }, [mode, asks, bids, depth]);

  const { maxSize, totalVisibleAsks, totalVisibleBids } = useMemo(
    () => summarizeDisplay(display),
    [display]
  );

  const reversedAsks = useMemo(() => display.asks.slice().reverse(), [display.asks]);

  return (
    <aside className="book-panel">
      <div className="pane-title">ORDER BOOK · DEPTH 100</div>
      <div className="dom-summary">
        <span className="sell">Sell Limits: {fmtQty(totalVisibleAsks)}</span>
        <span className="buy">Buy Limits: {fmtQty(totalVisibleBids)}</span>
      </div>
      <div className="dom-mode-toggle">
        <button type="button" className={mode === 'raw' ? 'active' : ''} onClick={() => setMode('raw')}>Raw Levels</button>
        <button type="button" className={mode === 'agg' ? 'active' : ''} onClick={() => setMode('agg')}>$1 Aggregation</button>
      </div>
      <div className="book-columns"><span>Price</span><span>Size</span><span>Notional</span></div>
      <div className="book-scroll">
        {reversedAsks.map((level) => (
          <OrderBookRow key={`a-${level.price}`} side="ask" level={level} maxSize={maxSize} />
        ))}
        <div className="spread-row">
          <span>Spread</span>
          <span>{depth?.spread ? depth.spread.toFixed(2) : '--'}</span>
          <span>{depth?.bestBid && depth?.bestAsk ? `${fmtPrice(depth.bestBid.price)} / ${fmtPrice(depth.bestAsk.price)}` : '--'}</span>
        </div>
        {display.bids.map((level) => (
          <OrderBookRow key={`b-${level.price}`} side="bid" level={level} maxSize={maxSize} />
        ))}
      </div>
    </aside>
  );
}

export const OrderBookLadder = memo(OrderBookLadderComponent);

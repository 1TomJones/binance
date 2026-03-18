import React, { memo, useMemo } from 'react';

function fmtTime(ts) {
  const d = new Date(ts);
  return `${d.toLocaleTimeString()}.${String(d.getMilliseconds()).padStart(3, '0')}`;
}

const TapeRow = memo(function TapeRow({ trade }) {
  const notional = trade.price * trade.quantity;

  return (
    <div className={`tape-row ${trade.side}`}>
      <span>{fmtTime(trade.trade_time)}</span>
      <span>{trade.trade_id}</span>
      <span>{trade.price.toFixed(2)}</span>
      <span>{trade.quantity.toFixed(4)}</span>
      <span>{notional.toFixed(2)}</span>
      <span className="side-tag">{trade.side.toUpperCase()}</span>
    </div>
  );
}, (prev, next) => prev.trade === next.trade);

function TradeTapeComponent({ trades }) {
  const rows = useMemo(() => trades.map((trade) => ({
    key: `${trade.trade_id}-${trade.trade_time}`,
    trade
  })), [trades]);

  return (
    <aside className="tape-panel">
      <div className="pane-title">LIVE TRADE TAPE</div>
      <div className="tape-columns">
        <span>Time</span><span>ID</span><span>Price</span><span>Qty</span><span>Notional</span><span>Side</span>
      </div>
      <div className="tape-scroll">
        {rows.map(({ key, trade }) => (
          <TapeRow key={key} trade={trade} />
        ))}
      </div>
    </aside>
  );
}

export const TradeTape = memo(TradeTapeComponent);

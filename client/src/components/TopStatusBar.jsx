import React, { memo } from 'react';

function TopStatusBarComponent({
  mode,
  symbol = 'BTCUSDT',
  lastPrice,
  high,
  low,
  movePct,
  bid,
  ask,
  spread,
  connected = true,
  onToggleFullscreen,
  isFullscreen
}) {
  return (
    <header className="top-bar">
      <div className="brand">KENT INVEST TERMINAL</div>
      <div className="market-metrics">
        <span>{mode}</span>
        <span>{symbol}</span>
        <span>LAST {lastPrice?.toFixed(2) ?? '--'}</span>
        <span>BID {bid?.toFixed(2) ?? '--'}</span>
        <span>ASK {ask?.toFixed(2) ?? '--'}</span>
        <span>SPR {spread?.toFixed(2) ?? '--'}</span>
        <span>HIGH {high?.toFixed(2) ?? '--'}</span>
        <span>LOW {low?.toFixed(2) ?? '--'}</span>
        <span className={movePct >= 0 ? 'up' : 'down'}>MOVE {Number.isFinite(movePct) ? `${movePct.toFixed(2)}%` : '--'}</span>
        <span className={connected ? 'up' : 'down'}>{connected ? 'ONLINE' : 'RECONNECTING'}</span>
      </div>
      <div className="route-links">
        <a href="/">Live</a>
        <a href="/quant">Quant</a>
        {onToggleFullscreen && (
          <button type="button" className="fullscreen-btn" onClick={onToggleFullscreen}>
            {isFullscreen ? '⤢ Exit' : '⤢ Full'}
          </button>
        )}
      </div>
    </header>
  );
}

export const TopStatusBar = memo(TopStatusBarComponent);

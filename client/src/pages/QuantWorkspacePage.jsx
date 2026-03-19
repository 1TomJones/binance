import React, { useEffect, useMemo, useRef, useState } from 'react';
import { quantApi } from '../services/quantApi.js';

const FALLBACK_LIMITS = {
  orderSizeMin: 0.0001,
  orderSizeMax: 0.005,
  orderSizeStep: 0.0001,
  initialBalance: 10000
};

const DEFAULT_SETTINGS = {
  orderSize: 0.001,
  stopLossPct: 0.35,
  takeProfitPct: 0.7,
  enableLong: true,
  enableShort: true
};

export function QuantWorkspacePage() {
  const [catalog, setCatalog] = useState({ builtIn: [], uploaded: [] });
  const [limits, setLimits] = useState(FALLBACK_LIMITS);
  const [selection, setSelection] = useState({ kind: 'built_in', key: 'VWAP_CVD_Live_Trend_01' });
  const [settings, setSettings] = useState(DEFAULT_SETTINGS);
  const [snapshot, setSnapshot] = useState(null);
  const [loading, setLoading] = useState(true);
  const [liveBusy, setLiveBusy] = useState(false);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [liveError, setLiveError] = useState('');
  const [uploadMessage, setUploadMessage] = useState('');

  useEffect(() => {
    let mounted = true;

    async function loadInitial() {
      try {
        const [catalogPayload, workspacePayload] = await Promise.all([
          quantApi.getStrategyCatalog(),
          quantApi.getLiveWorkspace()
        ]);
        if (!mounted) return;

        setCatalog(catalogPayload.strategies || { builtIn: [], uploaded: [] });
        setLimits(catalogPayload.limits || FALLBACK_LIMITS);
        setSnapshot(workspacePayload.snapshot || null);
        setSettings((prev) => ({ ...prev, ...(workspacePayload.snapshot?.controls || {}) }));
      } catch (loadError) {
        if (mounted) setLiveError(loadError.message);
      } finally {
        if (mounted) setLoading(false);
      }
    }

    loadInitial();

    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    let active = true;

    const poll = setInterval(async () => {
      try {
        const payload = await quantApi.getLiveWorkspace();
        if (!active) return;
        setSnapshot(payload.snapshot || null);
      } catch (pollError) {
        if (active) setLiveError(pollError.message);
      }
    }, 2000);

    return () => {
      active = false;
      clearInterval(poll);
    };
  }, []);

  const strategyOptions = useMemo(() => {
    const builtIn = (catalog.builtIn || []).map((item) => ({ ...item, kind: 'built_in', value: `built_in:${item.key}` }));
    const uploaded = (catalog.uploaded || []).map((item) => ({ ...item, kind: 'uploaded', value: `uploaded:${item.id}` }));
    return { builtIn, uploaded, all: [...builtIn, ...uploaded] };
  }, [catalog]);

  const liveStrategy = useMemo(() => findStrategy(strategyOptions, selection), [strategyOptions, selection]);

  const status = snapshot?.status || 'idle';
  const isRunning = status === 'running';
  const effectiveSymbol = snapshot?.symbol || liveStrategy?.symbol || 'BTCUSDT';
  const position = snapshot?.position || buildFlatPosition();
  const performance = snapshot?.performance || buildEmptyPerformance();

  const handleNumberChange = (field) => (event) => {
    const value = Number(event.target.value);
    setSettings((prev) => ({ ...prev, [field]: value }));
  };

  const handleToggleChange = (field) => (event) => {
    setSettings((prev) => ({ ...prev, [field]: event.target.checked }));
  };

  async function refreshCatalogAndSelectUploaded(record) {
    const payload = await quantApi.getStrategyCatalog();
    setCatalog(payload.strategies || { builtIn: [], uploaded: [] });
    setLimits(payload.limits || FALLBACK_LIMITS);
    if (record?.id) {
      setSelection({ kind: 'uploaded', id: record.id });
    }
  }

  async function handleUpload(file) {
    if (!file) return;
    setUploadBusy(true);
    setLiveError('');
    setUploadMessage('');
    try {
      const content = await file.text();
      const payload = await quantApi.uploadStrategy({ fileName: file.name, content });
      await refreshCatalogAndSelectUploaded(payload.strategy);
      setUploadMessage(`Uploaded ${file.name} successfully.`);
    } catch (uploadError) {
      setLiveError(uploadError.message);
    } finally {
      setUploadBusy(false);
    }
  }

  const startStrategy = async () => {
    if (!liveStrategy) return;
    setLiveBusy(true);
    setLiveError('');
    try {
      const payload = await quantApi.startLivePaper({
        strategyRef: selection,
        runConfig: settings
      });
      setSnapshot(payload.run || null);
    } catch (startError) {
      setLiveError(startError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  const stopStrategy = async () => {
    setLiveBusy(true);
    setLiveError('');
    try {
      const payload = await quantApi.stopLivePaper();
      setSnapshot(payload.run || null);
    } catch (stopError) {
      setLiveError(stopError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  return (
    <main className="quant-shell">
      <header className="quant-hero">
        <div>
          <p className="quant-kicker">Quant</p>
          <h1>Professional paper execution for live market strategy monitoring.</h1>
          <p className="quant-subtitle">
            The Quant tab now focuses entirely on live paper trading, strategy uploads, execution controls, and real-time monitoring.
          </p>
        </div>
        <div className="quant-hero-badges">
          <span className="quant-pill">{effectiveSymbol}</span>
          <span className={`quant-pill ${isRunning ? 'is-accent' : ''}`}>{isRunning ? 'Live Running' : 'Live Stopped'}</span>
        </div>
      </header>

      {liveError ? <div className="quant-banner quant-banner-error">{liveError}</div> : null}
      {uploadMessage ? <div className="quant-banner">{uploadMessage}</div> : null}
      {loading ? <div className="quant-banner">Loading Quant workspace…</div> : null}

      <section className="quant-mode-grid">
        <div className="quant-stack">
          <section className="quant-card quant-card-hero">
            <div className="quant-card-header">
              <div>
                <h3>Live paper strategy</h3>
                <span>Clean execution controls with the active strategy source visible at all times.</span>
              </div>
              <StatusBadge active={isRunning}>{isRunning ? 'Running' : 'Stopped'}</StatusBadge>
            </div>

            <StrategySourcePanel
              title="Strategy source"
              description="Choose a built-in strategy or drop in a JSON strategy file."
              options={strategyOptions}
              selection={selection}
              onSelectionChange={setSelection}
              onUpload={handleUpload}
              busy={uploadBusy || isRunning || liveBusy}
              activeStrategy={liveStrategy}
            />

            <div className="quant-control-grid">
              <Field label="Order size">
                <input type="number" min={limits.orderSizeMin} max={limits.orderSizeMax} step={limits.orderSizeStep} value={settings.orderSize} onChange={handleNumberChange('orderSize')} disabled={isRunning || liveBusy} />
                <small>{`${limits.orderSizeMin.toFixed(4)} to ${limits.orderSizeMax.toFixed(4)} BTC`}</small>
              </Field>
              <Field label="Stop loss %">
                <input type="number" min="0.01" max="25" step="0.01" value={settings.stopLossPct} onChange={handleNumberChange('stopLossPct')} disabled={isRunning || liveBusy} />
              </Field>
              <Field label="Take profit %">
                <input type="number" min="0.01" max="25" step="0.01" value={settings.takeProfitPct} onChange={handleNumberChange('takeProfitPct')} disabled={isRunning || liveBusy} />
              </Field>
              <ToggleField label="Enable long trades" checked={settings.enableLong} onChange={handleToggleChange('enableLong')} disabled={isRunning || liveBusy} />
              <ToggleField label="Enable short trades" checked={settings.enableShort} onChange={handleToggleChange('enableShort')} disabled={isRunning || liveBusy} />
              <div className="quant-action-row">
                <button className="quant-button quant-button-primary" onClick={startStrategy} disabled={isRunning || liveBusy || loading || !liveStrategy}>Start live</button>
                <button className="quant-button" onClick={stopStrategy} disabled={!isRunning || liveBusy}>Stop</button>
              </div>
            </div>
          </section>

          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Live strategy chart</h3>
                <span>{snapshot?.strategy?.timeframe || liveStrategy?.timeframe || '1m'} · paper execution markers</span>
              </div>
            </div>
            <MiniStrategyChart chart={snapshot?.chart} />
          </section>
        </div>

        <div className="quant-stack">
          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Live position</h3>
                <span>{position.state}</span>
              </div>
            </div>
            <MetricGrid items={[
              ['Position state', position.state],
              ['Position size', formatQty(position.size)],
              ['Entry price', formatPrice(position.entryPrice)],
              ['Current mark', formatPrice(position.currentMarkPrice)],
              ['Notional exposure', formatMoney(position.notionalExposure)],
              ['Unrealized PnL', formatMoney(position.unrealizedPnl)],
              ['Realized PnL', formatMoney(performance.cumulativeRealizedPnl)],
              ['Total PnL', formatMoney(performance.totalPnl)],
              ['Last action', snapshot?.lastAction || 'Stopped'],
              ['Strategy status', snapshot?.strategyStatus || 'Stopped']
            ]} />
          </section>

          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Strategy context</h3>
                <span>{liveStrategy?.name || snapshot?.strategy?.name || 'Strategy'}</span>
              </div>
            </div>
            <div className="quant-rule-list">
              <RuleBlock label="Description" value={liveStrategy?.description || snapshot?.strategy?.description || 'Strategy description unavailable.'} />
              <RuleBlock label="Long entry" value={readRule(liveStrategy?.entryRules?.long || snapshot?.strategy?.entryRules?.long)} />
              <RuleBlock label="Short entry" value={readRule(liveStrategy?.entryRules?.short || snapshot?.strategy?.entryRules?.short)} />
              <RuleBlock label="Long exit" value={readRule(liveStrategy?.exitRules?.long || snapshot?.strategy?.exitRules?.long)} />
              <RuleBlock label="Short exit" value={readRule(liveStrategy?.exitRules?.short || snapshot?.strategy?.exitRules?.short)} />
            </div>
          </section>

          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Live performance</h3>
                <span>Updates while the strategy runs</span>
              </div>
            </div>
            <MetricGrid items={[
              ['Total trades', performance.totalTrades],
              ['Wins', performance.wins],
              ['Losses', performance.losses],
              ['Win rate', `${formatNumber(performance.winRate)}%`],
              ['Best trade', formatMoney(performance.bestTrade)],
              ['Worst trade', formatMoney(performance.worstTrade)],
              ['Average trade', formatMoney(performance.averageTrade)],
              ['Cumulative realized', formatMoney(performance.cumulativeRealizedPnl)],
              ['Cumulative unrealized', formatMoney(performance.cumulativeUnrealizedPnl)],
              ['Total return', `${formatNumber(performance.totalReturn)}%`]
            ]} />
          </section>
        </div>

        <section className="quant-card quant-full-width">
          <div className="quant-card-header">
            <div>
              <h3>Live trade log</h3>
              <span>{snapshot?.tradeLog?.length || 0} recent fills</span>
            </div>
          </div>
          <LiveTradeLogTable rows={snapshot?.tradeLog || []} />
        </section>
      </section>
    </main>
  );
}

function StrategySourcePanel({ title, description, options, selection, onSelectionChange, onUpload, busy, activeStrategy }) {
  const inputRef = useRef(null);

  const handleSelectChange = (event) => {
    const [kind, rawValue] = event.target.value.split(':');
    onSelectionChange(kind === 'uploaded' ? { kind, id: Number(rawValue) } : { kind, key: rawValue });
  };

  const handleDrop = async (event) => {
    event.preventDefault();
    const file = event.dataTransfer.files?.[0];
    if (file) await onUpload(file);
  };

  return (
    <div className="quant-source-panel">
      <div className="quant-source-copy">
        <p className="quant-section-label">{title}</p>
        <h4>{activeStrategy?.name || 'Select a strategy'}</h4>
        <p>{description}</p>
      </div>
      <div className="quant-source-controls">
        <Field label="Built-in or uploaded strategy">
          <select value={selection.kind === 'uploaded' ? `uploaded:${selection.id}` : `built_in:${selection.key}`} onChange={handleSelectChange} disabled={busy}>
            <optgroup label="Built-in strategies">
              {options.builtIn.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
            </optgroup>
            {options.uploaded.length ? (
              <optgroup label="Uploaded strategies">
                {options.uploaded.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
              </optgroup>
            ) : null}
          </select>
        </Field>
        <button className="quant-button" type="button" onClick={() => inputRef.current?.click()} disabled={busy}>Upload strategy file</button>
      </div>
      <div className={`quant-dropzone ${busy ? 'is-disabled' : ''}`} onDragOver={(event) => event.preventDefault()} onDrop={handleDrop} onClick={() => !busy && inputRef.current?.click()} role="button" tabIndex={0}>
        <strong>Drag and drop a JSON strategy here</strong>
        <span>Or click to browse. Uploaded strategies become selectable for live paper execution immediately.</span>
      </div>
      <input ref={inputRef} className="quant-hidden-input" type="file" accept=".json,application/json" onChange={(event) => onUpload(event.target.files?.[0])} />
      {activeStrategy ? (
        <div className="quant-strategy-meta">
          <span>{activeStrategy.source === 'uploaded' ? 'Uploaded strategy' : 'Built-in strategy'}</span>
          <span>{activeStrategy.symbol}</span>
          <span>{activeStrategy.timeframe}</span>
        </div>
      ) : null}
    </div>
  );
}

function LiveTradeLogTable({ rows }) {
  if (!rows.length) {
    return <p className="quant-empty">No paper fills yet. Start the strategy to populate the live log.</p>;
  }

  return (
    <div className="quant-table-wrap">
      <table className="quant-table">
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Action</th>
            <th>Side</th>
            <th>Size</th>
            <th>Fill price</th>
            <th>Reason</th>
            <th>Position</th>
            <th>Realized PnL</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td>{formatTimestamp(row.timestamp)}</td>
              <td>{row.action}</td>
              <td>{row.side ? row.side.toUpperCase() : '—'}</td>
              <td>{formatQty(row.size)}</td>
              <td>{formatPrice(row.fillPrice)}</td>
              <td className="quant-log-reason">{humanizeExitReason(row.reason)}</td>
              <td>{row.resultingPosition}</td>
              <td>{row.realizedPnl == null ? '—' : formatMoney(row.realizedPnl)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MetricGrid({ items }) {
  return (
    <div className="quant-metric-grid">
      {items.map(([label, value]) => (
        <div key={label} className="quant-metric-cell">
          <label>{label}</label>
          <strong>{String(value ?? '—')}</strong>
        </div>
      ))}
    </div>
  );
}

function Field({ label, children }) {
  return (
    <label className="quant-field">
      <span>{label}</span>
      {children}
    </label>
  );
}

function ToggleField({ label, ...props }) {
  return (
    <label className="quant-toggle-field">
      <span>{label}</span>
      <input type="checkbox" {...props} />
    </label>
  );
}

function RuleBlock({ label, value }) {
  return (
    <div>
      <label>{label}</label>
      <p>{value}</p>
    </div>
  );
}

function StatusBadge({ active, children }) {
  return <span className={`quant-status-badge ${active ? 'is-positive' : ''}`}>{children}</span>;
}

function MiniStrategyChart({ chart }) {
  const candles = chart?.candles || [];
  if (!candles.length) {
    return <p className="quant-empty">Waiting for live candles to build the strategy chart.</p>;
  }

  const width = 980;
  const height = 300;
  const padding = { top: 16, right: 16, bottom: 28, left: 16 };
  const minPrice = Math.min(...candles.map((candle) => candle.low), ...(chart.averageEntryPrice ? [chart.averageEntryPrice] : []));
  const maxPrice = Math.max(...candles.map((candle) => candle.high), ...(chart.averageEntryPrice ? [chart.averageEntryPrice] : []));
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const candleStep = plotWidth / Math.max(candles.length, 1);
  const bodyWidth = Math.max(candleStep * 0.56, 2);
  const priceRange = Math.max(maxPrice - minPrice, 1);

  const xForIndex = (index) => padding.left + candleStep * index + candleStep / 2;
  const yForPrice = (price) => padding.top + ((maxPrice - price) / priceRange) * plotHeight;
  const vwapPath = candles.map((candle, index) => `${index === 0 ? 'M' : 'L'} ${xForIndex(index)} ${yForPrice(candle.vwap)}`).join(' ');
  const markerMap = new Map(candles.map((candle, index) => [candle.time, { candle, index }]));

  return (
    <div className="quant-mini-chart">
      <svg viewBox={`0 0 ${width} ${height}`} className="quant-chart-svg" role="img" aria-label="Live strategy candlestick chart">
        <rect x="0" y="0" width={width} height={height} fill="#07101c" />
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#17243a" strokeWidth="1" />
        <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#17243a" strokeWidth="1" />
        <path d={vwapPath} fill="none" stroke="#89aef7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

        {candles.map((candle, index) => {
          const x = xForIndex(index);
          const openY = yForPrice(candle.open);
          const closeY = yForPrice(candle.close);
          const highY = yForPrice(candle.high);
          const lowY = yForPrice(candle.low);
          const color = candle.close >= candle.open ? '#47c28f' : '#e16a74';
          const bodyTop = Math.min(openY, closeY);
          const bodyHeight = Math.max(Math.abs(closeY - openY), 1.5);
          return (
            <g key={candle.time}>
              <line x1={x} y1={highY} x2={x} y2={lowY} stroke={color} strokeWidth="1.5" />
              <rect x={x - bodyWidth / 2} y={bodyTop} width={bodyWidth} height={bodyHeight} fill={color} rx="1" />
            </g>
          );
        })}

        {(chart.markers || []).map((marker, index) => {
          const mapped = markerMap.get(marker.time);
          if (!mapped) return null;
          const x = xForIndex(mapped.index);
          const candle = mapped.candle;
          if (marker.action === 'BUY') {
            const y = yForPrice(candle.low) + 10;
            return <path key={`${marker.time}-${index}`} d={`M ${x} ${y - 16} L ${x - 7} ${y - 2} L ${x + 7} ${y - 2} Z`} fill="#47c28f" />;
          }
          if (marker.action === 'SELL') {
            const y = yForPrice(candle.high) - 10;
            return <path key={`${marker.time}-${index}`} d={`M ${x} ${y + 16} L ${x - 7} ${y + 2} L ${x + 7} ${y + 2} Z`} fill="#e16a74" />;
          }
          const y = yForPrice(marker.price);
          return <circle key={`${marker.time}-${index}`} cx={x} cy={y} r="5" fill="#d8b04d" stroke="#07101c" strokeWidth="2" />;
        })}
      </svg>
      <div className="quant-chart-legend">
        <span><i className="is-buy" /> Buy entry</span>
        <span><i className="is-sell" /> Sell entry</span>
        <span><i className="is-exit" /> Exit</span>
        <span><i className="is-vwap" /> VWAP</span>
      </div>
    </div>
  );
}

function findStrategy(strategyOptions, selection) {
  return strategyOptions.all.find((item) => (
    selection.kind === 'uploaded' ? item.kind === 'uploaded' && Number(item.id) === Number(selection.id) : item.kind === 'built_in' && item.key === selection.key
  )) || strategyOptions.all[0] || null;
}

function buildEmptyPerformance() {
  return {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    winRate: 0,
    bestTrade: 0,
    worstTrade: 0,
    averageTrade: 0,
    cumulativeRealizedPnl: 0,
    cumulativeUnrealizedPnl: 0,
    totalPnl: 0,
    totalReturn: 0
  };
}

function buildFlatPosition() {
  return {
    state: 'Flat',
    size: 0,
    entryPrice: null,
    currentMarkPrice: null,
    notionalExposure: 0,
    unrealizedPnl: 0
  };
}

function readRule(value) {
  if (!value) return '—';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function humanizeExitReason(value) {
  const labels = {
    stop_loss: 'Stop loss',
    take_profit: 'Take profit',
    signal_exit: 'Signal exit',
    end_of_day_exit: 'End of day exit',
    max_holding_bars: 'Max holding bars'
  };
  return labels[value] || value || '—';
}

function formatTimestamp(value) {
  if (!value) return '—';
  return new Date(value).toLocaleString(undefined, { hour12: false });
}

function formatPrice(value) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatMoney(value) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  const number = Number(value);
  return `${number >= 0 ? '+' : ''}${number.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatQty(value) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(4);
}

function formatNumber(value) {
  if (value == null || Number.isNaN(Number(value))) return '0.00';
  return Number(value).toFixed(2);
}

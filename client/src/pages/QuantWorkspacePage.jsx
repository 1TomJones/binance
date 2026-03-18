import React, { useEffect, useMemo, useRef, useState } from 'react';
import { quantApi } from '../services/quantApi.js';

const FALLBACK_LIMITS = {
  orderSizeMin: 0.0001,
  orderSizeMax: 0.005,
  orderSizeStep: 0.0001,
  initialBalance: 10000,
  maxReplaySpeed: 60
};

const DEFAULT_SETTINGS = {
  orderSize: 0.001,
  stopLossPct: 0.35,
  takeProfitPct: 0.7,
  enableLong: true,
  enableShort: true
};

const DEFAULT_BACKTEST_CONFIG = {
  startDate: '',
  endDate: '',
  replaySpeed: 60,
  initialBalance: 10000,
  ...DEFAULT_SETTINGS
};

export function QuantWorkspacePage() {
  const [mode, setMode] = useState('live');
  const [catalog, setCatalog] = useState({ builtIn: [], uploaded: [] });
  const [limits, setLimits] = useState(FALLBACK_LIMITS);
  const [liveSelection, setLiveSelection] = useState({ kind: 'built_in', key: 'VWAP_CVD_Live_Trend_01' });
  const [backtestSelection, setBacktestSelection] = useState({ kind: 'built_in', key: 'VWAP_CVD_Live_Trend_01' });
  const [settings, setSettings] = useState(DEFAULT_SETTINGS);
  const [snapshot, setSnapshot] = useState(null);
  const [backtestConfig, setBacktestConfig] = useState(DEFAULT_BACKTEST_CONFIG);
  const [backtestJob, setBacktestJob] = useState(null);
  const [backtestResult, setBacktestResult] = useState(null);
  const [analysisView, setAnalysisView] = useState('outcome');
  const [loading, setLoading] = useState(true);
  const [liveBusy, setLiveBusy] = useState(false);
  const [backtestBusy, setBacktestBusy] = useState(false);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [error, setError] = useState('');
  const [uploadMessage, setUploadMessage] = useState('');
  const jobPollRef = useRef(null);

  useEffect(() => {
    let mounted = true;

    async function loadInitial() {
      try {
        const [catalogPayload, workspacePayload] = await Promise.all([
          quantApi.getStrategyCatalog(),
          quantApi.getLiveWorkspace()
        ]);
        if (!mounted) return;

        const today = new Date();
        const endDate = toDateInput(today);
        const start = new Date(today.getTime() - 6 * 86400000);

        setCatalog(catalogPayload.strategies || { builtIn: [], uploaded: [] });
        setLimits(catalogPayload.limits || FALLBACK_LIMITS);
        setSnapshot(workspacePayload.snapshot || null);
        setSettings((prev) => ({ ...prev, ...(workspacePayload.snapshot?.controls || {}) }));
        setBacktestConfig((prev) => ({
          ...prev,
          initialBalance: catalogPayload.limits?.initialBalance || FALLBACK_LIMITS.initialBalance,
          startDate: prev.startDate || toDateInput(start),
          endDate: prev.endDate || endDate
        }));
      } catch (loadError) {
        if (mounted) setError(loadError.message);
      } finally {
        if (mounted) setLoading(false);
      }
    }

    loadInitial();

    const poll = setInterval(async () => {
      try {
        const payload = await quantApi.getLiveWorkspace();
        if (!mounted) return;
        setSnapshot(payload.snapshot || null);
      } catch (pollError) {
        if (mounted) setError(pollError.message);
      }
    }, 2000);

    return () => {
      mounted = false;
      clearInterval(poll);
      clearInterval(jobPollRef.current);
    };
  }, []);

  const strategyOptions = useMemo(() => {
    const builtIn = (catalog.builtIn || []).map((item) => ({ ...item, kind: 'built_in', value: `built_in:${item.key}` }));
    const uploaded = (catalog.uploaded || []).map((item) => ({ ...item, kind: 'uploaded', value: `uploaded:${item.id}` }));
    return { builtIn, uploaded, all: [...builtIn, ...uploaded] };
  }, [catalog]);

  const liveStrategy = useMemo(() => findStrategy(strategyOptions, liveSelection), [strategyOptions, liveSelection]);
  const backtestStrategy = useMemo(() => findStrategy(strategyOptions, backtestSelection), [strategyOptions, backtestSelection]);

  const status = snapshot?.status || 'idle';
  const isRunning = status === 'running';
  const effectiveSymbol = snapshot?.symbol || liveStrategy?.symbol || 'BTCUSDT';
  const position = snapshot?.position || buildFlatPosition();
  const performance = snapshot?.performance || buildEmptyPerformance();
  const progress = deriveCurrentProgress(backtestJob);

  const handleNumberChange = (field) => (event) => {
    const value = Number(event.target.value);
    setSettings((prev) => ({ ...prev, [field]: value }));
  };

  const handleBacktestNumberChange = (field) => (event) => {
    const value = event.target.type === 'checkbox' ? event.target.checked : Number(event.target.value);
    setBacktestConfig((prev) => ({ ...prev, [field]: value }));
  };

  const handleBacktestDateChange = (field) => (event) => {
    setBacktestConfig((prev) => ({ ...prev, [field]: event.target.value }));
  };

  const handleToggleChange = (field) => (event) => {
    setSettings((prev) => ({ ...prev, [field]: event.target.checked }));
  };

  const handleBacktestToggleChange = (field) => (event) => {
    setBacktestConfig((prev) => ({ ...prev, [field]: event.target.checked }));
  };

  async function refreshCatalogAndSelectUploaded(record) {
    const payload = await quantApi.getStrategyCatalog();
    setCatalog(payload.strategies || { builtIn: [], uploaded: [] });
    setLimits(payload.limits || FALLBACK_LIMITS);
    if (record?.id) {
      const nextRef = { kind: 'uploaded', id: record.id };
      setLiveSelection(nextRef);
      setBacktestSelection(nextRef);
    }
  }

  async function handleUpload(file) {
    if (!file) return;
    setUploadBusy(true);
    setError('');
    setUploadMessage('');
    try {
      const content = await file.text();
      const payload = await quantApi.uploadStrategy({ fileName: file.name, content });
      await refreshCatalogAndSelectUploaded(payload.strategy);
      setUploadMessage(`Uploaded ${file.name} successfully.`);
    } catch (uploadError) {
      setError(uploadError.message);
    } finally {
      setUploadBusy(false);
    }
  }

  const startStrategy = async () => {
    if (!liveStrategy) return;
    setLiveBusy(true);
    setError('');
    try {
      const payload = await quantApi.startLivePaper({
        strategyRef: liveSelection,
        runConfig: settings
      });
      setSnapshot(payload.run || null);
    } catch (startError) {
      setError(startError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  const stopStrategy = async () => {
    setLiveBusy(true);
    setError('');
    try {
      const payload = await quantApi.stopLivePaper();
      setSnapshot(payload.run || null);
    } catch (stopError) {
      setError(stopError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  const startBacktest = async () => {
    if (!backtestStrategy) return;
    setBacktestBusy(true);
    setError('');
    setBacktestResult(null);
    try {
      const payload = await quantApi.startBacktest({
        strategyRef: backtestSelection,
        runConfig: backtestConfig
      });
      setBacktestJob(payload.job || { id: payload.jobId });
      startJobPolling(payload.jobId);
    } catch (startError) {
      setError(startError.message);
    } finally {
      setBacktestBusy(false);
    }
  };

  const cancelBacktest = async () => {
    if (!backtestJob?.id) return;
    setBacktestBusy(true);
    setError('');
    try {
      await quantApi.cancelBacktest(backtestJob.id);
      clearInterval(jobPollRef.current);
      const payload = await quantApi.getBacktestJob(backtestJob.id);
      setBacktestJob(payload.job || null);
    } catch (cancelError) {
      setError(cancelError.message);
    } finally {
      setBacktestBusy(false);
    }
  };

  const startJobPolling = (jobId) => {
    clearInterval(jobPollRef.current);

    const poll = async () => {
      try {
        const payload = await quantApi.getBacktestJob(jobId);
        setBacktestJob(payload.job || null);
        if (payload.result) {
          setBacktestResult(payload.result);
        }
        if (['completed', 'failed', 'cancelled'].includes(payload.job?.status)) {
          clearInterval(jobPollRef.current);
        }
      } catch (pollError) {
        setError(pollError.message);
        clearInterval(jobPollRef.current);
      }
    };

    poll();
    jobPollRef.current = setInterval(poll, 1200);
  };

  return (
    <main className="quant-shell">
      <header className="quant-hero">
        <div>
          <p className="quant-kicker">Quant</p>
          <h1>Professional paper execution and historical replay in one workspace.</h1>
          <p className="quant-subtitle">
            Switch clearly between live paper trading and UTC day-by-day backtests without leaving the Quant tab.
          </p>
        </div>
        <div className="quant-hero-badges">
          <span className="quant-pill">{effectiveSymbol}</span>
          <span className={`quant-pill ${mode === 'backtest' ? 'is-accent' : ''}`}>{mode === 'live' ? 'Live Mode' : 'Backtest Mode'}</span>
        </div>
      </header>

      <section className="quant-mode-switcher">
        <button className={mode === 'live' ? 'is-active' : ''} onClick={() => setMode('live')}>Live</button>
        <button className={mode === 'backtest' ? 'is-active' : ''} onClick={() => setMode('backtest')}>Backtest</button>
      </section>

      {error ? <div className="quant-banner quant-banner-error">{error}</div> : null}
      {uploadMessage ? <div className="quant-banner">{uploadMessage}</div> : null}
      {loading ? <div className="quant-banner">Loading Quant workspace…</div> : null}

      {mode === 'live' ? (
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
                selection={liveSelection}
                onSelectionChange={setLiveSelection}
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
      ) : (
        <section className="quant-stack">
          <section className="quant-card quant-card-hero">
            <div className="quant-card-header">
              <div>
                <h3>Backtest workspace</h3>
                <span>UTC day-by-day historical simulation with session resets at 00:00 UTC.</span>
              </div>
              <StatusBadge active={backtestJob?.status === 'running'}>
                {humanizeStatus(backtestJob?.status || 'ready')}
              </StatusBadge>
            </div>

            <div className="quant-form-grid">
              <StrategySourcePanel
                title="Strategy source"
                description="Use the built-in strategy instantly or upload a JSON strategy for backtesting."
                options={strategyOptions}
                selection={backtestSelection}
                onSelectionChange={setBacktestSelection}
                onUpload={handleUpload}
                busy={uploadBusy || backtestBusy || backtestJob?.status === 'running'}
                activeStrategy={backtestStrategy}
              />

              <div className="quant-backtest-controls">
                <Field label="Start date">
                  <input type="date" value={backtestConfig.startDate} onChange={handleBacktestDateChange('startDate')} max={backtestConfig.endDate || undefined} />
                  <small>Every day starts from 00:00 UTC.</small>
                </Field>
                <Field label="End date">
                  <input type="date" value={backtestConfig.endDate} onChange={handleBacktestDateChange('endDate')} min={backtestConfig.startDate || undefined} />
                  <small>Open positions flatten at end of each UTC day.</small>
                </Field>
                <Field label="Order size">
                  <input type="number" min={limits.orderSizeMin} max={limits.orderSizeMax} step={limits.orderSizeStep} value={backtestConfig.orderSize} onChange={handleBacktestNumberChange('orderSize')} />
                  <small>{`${limits.orderSizeMin.toFixed(4)} to ${limits.orderSizeMax.toFixed(4)} BTC`}</small>
                </Field>
                <Field label="Initial balance">
                  <input type="number" min="100" step="100" value={backtestConfig.initialBalance} onChange={handleBacktestNumberChange('initialBalance')} />
                </Field>
                <Field label="Replay speed target">
                  <input type="range" min="1" max={limits.maxReplaySpeed || 60} step="1" value={backtestConfig.replaySpeed} onChange={handleBacktestNumberChange('replaySpeed')} />
                  <small>{backtestConfig.replaySpeed}x target · backend runs as fast as practical.</small>
                </Field>
                <Field label="Stop loss %">
                  <input type="number" min="0.01" max="25" step="0.01" value={backtestConfig.stopLossPct} onChange={handleBacktestNumberChange('stopLossPct')} />
                </Field>
                <Field label="Take profit %">
                  <input type="number" min="0.01" max="25" step="0.01" value={backtestConfig.takeProfitPct} onChange={handleBacktestNumberChange('takeProfitPct')} />
                </Field>
                <ToggleField label="Longs enabled" checked={backtestConfig.enableLong} onChange={handleBacktestToggleChange('enableLong')} />
                <ToggleField label="Shorts enabled" checked={backtestConfig.enableShort} onChange={handleBacktestToggleChange('enableShort')} />
                <div className="quant-action-row">
                  <button className="quant-button quant-button-primary" onClick={startBacktest} disabled={backtestBusy || !backtestStrategy || !backtestConfig.startDate || !backtestConfig.endDate}>Start backtest</button>
                  <button className="quant-button" onClick={cancelBacktest} disabled={backtestJob?.status !== 'running' || backtestBusy}>Stop</button>
                </div>
              </div>
            </div>
          </section>

          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Backtest progress</h3>
                <span>The UI remains responsive while the replay runs.</span>
              </div>
            </div>
            <ProgressPanel progress={progress} />
          </section>

          {backtestResult ? (
            <>
              <section className="quant-card">
                <div className="quant-card-header">
                  <div>
                    <h3>Main PnL chart</h3>
                    <span>Cumulative realized PnL after every closed trade</span>
                  </div>
                </div>
                <PnlLineChart points={backtestResult.series?.cumulativePnlSeries || []} />
              </section>

              <section className="quant-results-grid">
                <section className="quant-card">
                  <div className="quant-card-header">
                    <div>
                      <h3>Backtest metrics</h3>
                      <span>Core strategy analytics</span>
                    </div>
                  </div>
                  <MetricGrid items={buildBacktestMetrics(backtestResult.summary)} />
                </section>

                <section className="quant-card">
                  <div className="quant-card-header">
                    <div>
                      <h3>Analysis views</h3>
                      <span>Interactive slices of the completed run</span>
                    </div>
                    <select className="quant-inline-select" value={analysisView} onChange={(event) => setAnalysisView(event.target.value)}>
                      <option value="outcome">Outcome mix</option>
                      <option value="timeOfDay">Time of day</option>
                      <option value="duration">Trade duration</option>
                      <option value="exitReasons">Exit reasons</option>
                    </select>
                  </div>
                  <AnalysisPanel view={analysisView} analyses={backtestResult.summary?.analyses || {}} summary={backtestResult.summary || {}} />
                </section>
              </section>

              <section className="quant-card">
                <div className="quant-card-header">
                  <div>
                    <h3>Trade log</h3>
                    <span>{backtestResult.tradeLog?.length || 0} completed trades</span>
                  </div>
                </div>
                <BacktestTradeTable rows={backtestResult.tradeLog || []} />
              </section>
            </>
          ) : null}
        </section>
      )}
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
        <span>Or click to browse. Uploaded strategies become selectable in both Live and Backtest modes.</span>
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

function ProgressPanel({ progress }) {
  const pct = Math.max(0, Math.min(100, progress.percent || 0));
  return (
    <div className="quant-progress-panel">
      <div className="quant-progress-track">
        <div className="quant-progress-fill" style={{ width: `${pct}%` }} />
      </div>
      <div className="quant-progress-grid">
        <MetricCard label="Status" value={progress.status} />
        <MetricCard label="Current date" value={progress.currentDate} />
        <MetricCard label="Percent complete" value={`${formatNumber(pct)}%`} />
        <MetricCard label="Elapsed" value={formatDuration(progress.elapsedMs)} />
        <MetricCard label="Trades so far" value={progress.totalTrades} />
        <MetricCard label="Current marker" value={progress.marker} />
      </div>
    </div>
  );
}

function AnalysisPanel({ view, analyses, summary }) {
  if (view === 'outcome') {
    return <OutcomeChart outcome={analyses.outcome || { winning: 0, losing: 0, breakeven: 0 }} />;
  }
  if (view === 'timeOfDay') {
    return <BarChart rows={analyses.timeOfDay || []} dataKey="winRate" labelKey="label" title="Average win rate by UTC hour" suffix="%" />;
  }
  if (view === 'duration') {
    return (
      <div className="quant-analysis-stack">
        <MetricGrid items={[
          ['Average duration', `${formatNumber(summary.averageTradeDurationMinutes)} min`],
          ['Median duration', `${formatNumber(summary.medianTradeDurationMinutes)} min`]
        ]} />
        <BarChart rows={analyses.durationBuckets || []} dataKey="count" labelKey="key" title="Trade duration distribution" />
      </div>
    );
  }
  return <BarChart rows={analyses.exitReasons || []} dataKey="count" labelKey="reason" title="Exit reason count" />;
}

function OutcomeChart({ outcome }) {
  const total = Math.max((outcome.winning || 0) + (outcome.losing || 0) + (outcome.breakeven || 0), 1);
  const winning = (outcome.winning || 0) / total * 100;
  const losing = (outcome.losing || 0) / total * 100;
  const breakeven = (outcome.breakeven || 0) / total * 100;
  const style = {
    background: `conic-gradient(#47c28f 0 ${winning}%, #e16a74 ${winning}% ${winning + losing}%, #7d8fab ${winning + losing}% 100%)`
  };

  return (
    <div className="quant-outcome-wrap">
      <div className="quant-outcome-donut" style={style}><div /></div>
      <div className="quant-outcome-legend">
        <LegendRow color="#47c28f" label="Winning trades" value={outcome.winning || 0} />
        <LegendRow color="#e16a74" label="Losing trades" value={outcome.losing || 0} />
        <LegendRow color="#7d8fab" label="Breakeven trades" value={outcome.breakeven || 0} />
      </div>
    </div>
  );
}

function BarChart({ rows, dataKey, labelKey, title, suffix = '' }) {
  if (!rows.length) {
    return <p className="quant-empty">No data available for this analysis yet.</p>;
  }

  const maxValue = Math.max(...rows.map((row) => Number(row[dataKey] || 0)), 1);
  return (
    <div className="quant-bar-chart">
      <p className="quant-chart-title">{title}</p>
      {rows.map((row) => (
        <div key={row[labelKey]} className="quant-bar-row">
          <span>{row[labelKey]}</span>
          <div className="quant-bar-track"><div className="quant-bar-fill" style={{ width: `${(Number(row[dataKey] || 0) / maxValue) * 100}%` }} /></div>
          <strong>{formatNumber(row[dataKey])}{suffix}</strong>
        </div>
      ))}
    </div>
  );
}

function PnlLineChart({ points }) {
  if (!points.length) {
    return <p className="quant-empty">Run a completed backtest to render the cumulative realized PnL curve.</p>;
  }

  const width = 980;
  const height = 320;
  const padding = { top: 20, right: 20, bottom: 28, left: 56 };
  const values = points.map((point) => Number(point.cumulativeRealizedPnl || 0));
  const min = Math.min(...values, 0);
  const max = Math.max(...values, 0);
  const range = Math.max(max - min, 1);
  const xForIndex = (index) => padding.left + (index / Math.max(points.length - 1, 1)) * (width - padding.left - padding.right);
  const yForValue = (value) => padding.top + ((max - value) / range) * (height - padding.top - padding.bottom);
  const path = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${xForIndex(index)} ${yForValue(point.cumulativeRealizedPnl)}`).join(' ');

  return (
    <div className="quant-mini-chart">
      <svg viewBox={`0 0 ${width} ${height}`} className="quant-chart-svg" role="img" aria-label="Cumulative realized PnL chart">
        <rect width={width} height={height} fill="#07101c" />
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#17243a" />
        <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#17243a" />
        {[min, (min + max) / 2, max].map((value) => (
          <g key={value}>
            <line x1={padding.left} y1={yForValue(value)} x2={width - padding.right} y2={yForValue(value)} stroke="#111d31" strokeDasharray="4 6" />
            <text x={8} y={yForValue(value) + 4} fill="#6f84aa" fontSize="11">{formatMoney(value)}</text>
          </g>
        ))}
        <path d={path} fill="none" stroke="#7eb2ff" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
        {points.map((point, index) => (
          <circle key={`${point.index}-${point.time}`} cx={xForIndex(index)} cy={yForValue(point.cumulativeRealizedPnl)} r="3" fill={point.cumulativeRealizedPnl >= 0 ? '#47c28f' : '#e16a74'} />
        ))}
      </svg>
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

function BacktestTradeTable({ rows }) {
  if (!rows.length) {
    return <p className="quant-empty">Completed trades will appear here once the replay finishes.</p>;
  }

  return (
    <div className="quant-table-wrap quant-table-wrap-tall">
      <table className="quant-table">
        <thead>
          <tr>
            <th>Entry time</th>
            <th>Exit time</th>
            <th>Side</th>
            <th>Size</th>
            <th>Entry price</th>
            <th>Exit price</th>
            <th>Realized PnL</th>
            <th>Duration</th>
            <th>Exit reason</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${row.entryTime}-${row.exitTime}-${index}`}>
              <td>{formatTimestamp(row.entryTime)}</td>
              <td>{formatTimestamp(row.exitTime)}</td>
              <td>{row.side?.toUpperCase()}</td>
              <td>{formatQty(row.quantity)}</td>
              <td>{formatPrice(row.entryPrice)}</td>
              <td>{formatPrice(row.exitPrice)}</td>
              <td>{formatMoney(row.realizedPnl)}</td>
              <td>{formatDuration(row.durationMs)}</td>
              <td>{humanizeExitReason(row.exitReason)}</td>
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

function MetricCard({ label, value }) {
  return (
    <div className="quant-metric-cell">
      <label>{label}</label>
      <strong>{String(value ?? '—')}</strong>
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

function LegendRow({ color, label, value }) {
  return <div className="quant-legend-row"><i style={{ background: color }} /> <span>{label}</span><strong>{value}</strong></div>;
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

function deriveCurrentProgress(job) {
  const latest = job?.status ? job : null;
  return {
    status: humanizeStatus(latest?.status || 'ready'),
    currentDate: parseCurrentDate(latest?.current_marker) || '—',
    percent: latest?.progress_pct || 0,
    elapsedMs: latest?.elapsed_ms || 0,
    totalTrades: parseTradeCount(latest?.current_marker),
    marker: latest?.current_marker || 'Waiting to start'
  };
}

function buildBacktestMetrics(summary = {}) {
  return [
    ['Total trades', summary.totalTrades || 0],
    ['Wins', summary.wins || 0],
    ['Losses', summary.losses || 0],
    ['Win rate', `${formatNumber(summary.winRate)}%`],
    ['Cumulative realized PnL', formatMoney(summary.cumulativeRealizedPnl)],
    ['Average trade PnL', formatMoney(summary.averageTradePnl)],
    ['Best trade', formatMoney(summary.bestTrade)],
    ['Worst trade', formatMoney(summary.worstTrade)],
    ['Average duration', `${formatNumber(summary.averageTradeDurationMinutes)} min`],
    ['Median duration', `${formatNumber(summary.medianTradeDurationMinutes)} min`],
    ['Max drawdown', `${formatNumber(summary.maxDrawdown)}%`],
    ['Profit factor', formatNumber(summary.profitFactor)],
    ['Expectancy', formatMoney(summary.expectancy)]
  ];
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

function humanizeStatus(value) {
  const map = {
    idle: 'Idle',
    ready: 'Ready',
    queued: 'Queued',
    running: 'Running',
    completed: 'Completed',
    failed: 'Failed',
    cancelled: 'Cancelled'
  };
  return map[value] || value || 'Ready';
}

function parseCurrentDate(marker) {
  return marker?.match(/\d{4}-\d{2}-\d{2}/)?.[0] || null;
}

function parseTradeCount(marker) {
  return Number(marker?.match(/(\d+) trades/)?.[1] || 0);
}

function toDateInput(date) {
  return new Date(date).toISOString().slice(0, 10);
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

function formatDuration(value) {
  const ms = Number(value || 0);
  if (!ms) return '0s';
  const totalSeconds = Math.max(Math.floor(ms / 1000), 0);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes >= 60) {
    const hours = Math.floor(minutes / 60);
    return `${hours}h ${minutes % 60}m`;
  }
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

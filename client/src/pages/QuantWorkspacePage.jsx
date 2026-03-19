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

const DEFAULT_SPEEDS = [
  { key: 'step', label: 'Step' },
  { key: 'steady', label: 'Steady' },
  { key: 'fast', label: 'Fast' },
  { key: 'turbo', label: 'Turbo' }
];

const ANALYSIS_VIEWS = [
  { key: 'timeOfDay', label: 'Time of day' },
  { key: 'outcome', label: 'Outcome breakdown' },
  { key: 'duration', label: 'Trade duration' }
];

export function QuantWorkspacePage() {
  const [activeMode, setActiveMode] = useState('backtest');
  const [catalog, setCatalog] = useState({ builtIn: [], uploaded: [] });
  const [limits, setLimits] = useState(FALLBACK_LIMITS);
  const [selection, setSelection] = useState({ kind: 'built_in', key: 'VWAP_CVD_Live_Trend_01' });
  const [settings, setSettings] = useState(DEFAULT_SETTINGS);
  const [liveSnapshot, setLiveSnapshot] = useState(null);
  const [backtestSnapshot, setBacktestSnapshot] = useState(null);
  const [speeds, setSpeeds] = useState(DEFAULT_SPEEDS);
  const [backtestConfig, setBacktestConfig] = useState(() => buildDefaultBacktestConfig());
  const [analysisView, setAnalysisView] = useState('timeOfDay');
  const [loading, setLoading] = useState(true);
  const [liveBusy, setLiveBusy] = useState(false);
  const [backtestBusy, setBacktestBusy] = useState(false);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');
  const [uploadMessage, setUploadMessage] = useState('');

  useEffect(() => {
    let mounted = true;

    async function loadInitial() {
      try {
        const [catalogPayload, livePayload, backtestPayload] = await Promise.all([
          quantApi.getStrategyCatalog(),
          quantApi.getLiveWorkspace(),
          quantApi.getBacktestSnapshot()
        ]);

        if (!mounted) return;
        setCatalog(catalogPayload.strategies || { builtIn: [], uploaded: [] });
        setLimits(catalogPayload.limits || FALLBACK_LIMITS);
        setLiveSnapshot(livePayload.snapshot || null);
        setBacktestSnapshot(backtestPayload.snapshot || null);
        setSpeeds(backtestPayload.speeds || DEFAULT_SPEEDS);
        if (backtestPayload.suggestedConfig) setBacktestConfig(backtestPayload.suggestedConfig);
        setSettings((prev) => ({ ...prev, ...(livePayload.snapshot?.controls || {}) }));
      } catch (loadError) {
        if (mounted) setErrorMessage(loadError.message);
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

    const livePoll = setInterval(async () => {
      try {
        const payload = await quantApi.getLiveWorkspace();
        if (active) setLiveSnapshot(payload.snapshot || null);
      } catch (pollError) {
        if (active && activeMode === 'live') setErrorMessage(pollError.message);
      }
    }, 2000);

    const backtestPoll = setInterval(async () => {
      try {
        const payload = await quantApi.getBacktestSnapshot();
        if (!active) return;
        setBacktestSnapshot(payload.snapshot || null);
        if (payload.speeds?.length) setSpeeds(payload.speeds);
      } catch (pollError) {
        if (active && activeMode === 'backtest') setErrorMessage(pollError.message);
      }
    }, 900);

    return () => {
      active = false;
      clearInterval(livePoll);
      clearInterval(backtestPoll);
    };
  }, [activeMode]);

  const strategyOptions = useMemo(() => {
    const builtIn = (catalog.builtIn || []).map((item) => ({ ...item, kind: 'built_in', value: `built_in:${item.key}` }));
    const uploaded = (catalog.uploaded || []).map((item) => ({ ...item, kind: 'uploaded', value: `uploaded:${item.id}` }));
    return { builtIn, uploaded, all: [...builtIn, ...uploaded] };
  }, [catalog]);

  const activeStrategy = useMemo(() => findStrategy(strategyOptions, selection), [strategyOptions, selection]);
  const liveStatus = liveSnapshot?.status || 'idle';
  const liveRunning = liveStatus === 'running';
  const backtestStatus = backtestSnapshot?.status || 'idle';
  const backtestRunning = backtestStatus === 'running' || backtestStatus === 'preparing';
  const livePosition = liveSnapshot?.position || buildFlatPosition();
  const livePerformance = liveSnapshot?.performance || buildEmptyPerformance();
  const backtestPosition = backtestSnapshot?.position || buildFlatPosition();
  const backtestPerformance = backtestSnapshot?.performance || buildEmptyPerformance();
  const backtestResults = backtestSnapshot?.results || buildEmptyResults();
  const strategyName = activeStrategy?.name || backtestSnapshot?.strategy?.name || liveSnapshot?.strategy?.name || 'Strategy';
  const symbol = activeStrategy?.symbol || backtestSnapshot?.strategy?.symbol || liveSnapshot?.symbol || 'BTCUSDT';

  useEffect(() => {
    if (backtestStatus === 'completed' || backtestStatus === 'cancelled') {
      setAnalysisView((current) => current || 'timeOfDay');
    }
  }, [backtestStatus]);

  const handleNumberChange = (field) => (event) => {
    const value = Number(event.target.value);
    setSettings((prev) => ({ ...prev, [field]: value }));
  };

  const handleToggleChange = (field) => (event) => {
    setSettings((prev) => ({ ...prev, [field]: event.target.checked }));
  };

  const handleBacktestConfigChange = (field) => (event) => {
    const value = event.target.type === 'checkbox' ? event.target.checked : event.target.value;
    setBacktestConfig((prev) => ({ ...prev, [field]: value }));
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
    setErrorMessage('');
    setUploadMessage('');

    try {
      const content = await file.text();
      const payload = await quantApi.uploadStrategy({ fileName: file.name, content });
      await refreshCatalogAndSelectUploaded(payload.strategy);
      setUploadMessage(`Uploaded ${file.name} successfully.`);
    } catch (uploadError) {
      setErrorMessage(uploadError.message);
    } finally {
      setUploadBusy(false);
    }
  }

  const startLive = async () => {
    if (!activeStrategy) return;
    setLiveBusy(true);
    setErrorMessage('');

    try {
      const payload = await quantApi.startLivePaper({
        strategyRef: selection,
        runConfig: settings
      });
      setLiveSnapshot(payload.run || null);
      setActiveMode('live');
    } catch (startError) {
      setErrorMessage(startError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  const stopLive = async () => {
    setLiveBusy(true);
    setErrorMessage('');

    try {
      const payload = await quantApi.stopLivePaper();
      setLiveSnapshot(payload.run || null);
    } catch (stopError) {
      setErrorMessage(stopError.message);
    } finally {
      setLiveBusy(false);
    }
  };

  const startBacktest = async () => {
    if (!activeStrategy) return;
    setBacktestBusy(true);
    setErrorMessage('');

    try {
      const payload = await quantApi.startBacktest({
        strategyRef: selection,
        startDate: backtestConfig.startDate,
        endDate: backtestConfig.endDate,
        speed: backtestConfig.speed,
        runConfig: settings
      });
      setBacktestSnapshot(payload.snapshot || null);
      setSpeeds(payload.speeds || DEFAULT_SPEEDS);
      if (payload.normalizedRange) {
        setBacktestConfig((prev) => ({
          ...prev,
          startDate: payload.normalizedRange.startDate || prev.startDate,
          endDate: payload.normalizedRange.endDate || prev.endDate
        }));
      }
      setActiveMode('backtest');
      setAnalysisView('timeOfDay');
    } catch (startError) {
      setErrorMessage(startError.message);
    } finally {
      setBacktestBusy(false);
    }
  };

  const stopBacktest = async () => {
    setBacktestBusy(true);
    setErrorMessage('');

    try {
      const payload = await quantApi.stopBacktest();
      setBacktestSnapshot(payload.snapshot || null);
      setSpeeds(payload.speeds || DEFAULT_SPEEDS);
    } catch (stopError) {
      setErrorMessage(stopError.message);
    } finally {
      setBacktestBusy(false);
    }
  };

  return (
    <main className="quant-shell">
      <header className="quant-hero quant-hero-quant">
        <div>
          <p className="quant-kicker">Quant workspace</p>
          <h1>Clean live execution and a rebuilt historical backtest terminal.</h1>
          <p className="quant-subtitle">
            The new Backtest tab runs day-by-day historical market sessions with progressive indicators, simulated execution,
            trade recording, and a dedicated results mode once the replay completes.
          </p>
        </div>
        <div className="quant-hero-badges">
          <span className="quant-pill">{symbol}</span>
          <span className={`quant-pill ${liveRunning ? 'is-accent' : ''}`}>{liveRunning ? 'Live running' : 'Live idle'}</span>
          <span className={`quant-pill ${backtestRunning ? 'is-accent' : ''}`}>{backtestRunning ? 'Backtest running' : humanizeStatus(backtestStatus)}</span>
        </div>
      </header>

      <WorkspaceTabs activeMode={activeMode} onChange={setActiveMode} liveRunning={liveRunning} backtestStatus={backtestStatus} />

      {errorMessage ? <div className="quant-banner quant-banner-error">{errorMessage}</div> : null}
      {uploadMessage ? <div className="quant-banner">{uploadMessage}</div> : null}
      {loading ? <div className="quant-banner">Loading Quant workspace…</div> : null}

      {activeMode === 'live' ? (
        <LiveWorkspace
          strategyOptions={strategyOptions}
          selection={selection}
          onSelectionChange={setSelection}
          onUpload={handleUpload}
          uploadBusy={uploadBusy}
          limits={limits}
          settings={settings}
          onNumberChange={handleNumberChange}
          onToggleChange={handleToggleChange}
          snapshot={liveSnapshot}
          activeStrategy={activeStrategy}
          livePosition={livePosition}
          livePerformance={livePerformance}
          onStart={startLive}
          onStop={stopLive}
          busy={liveBusy}
          loading={loading}
        />
      ) : (
        <BacktestWorkspace
          strategyOptions={strategyOptions}
          selection={selection}
          onSelectionChange={setSelection}
          onUpload={handleUpload}
          uploadBusy={uploadBusy}
          limits={limits}
          settings={settings}
          onNumberChange={handleNumberChange}
          onToggleChange={handleToggleChange}
          backtestConfig={backtestConfig}
          onBacktestConfigChange={handleBacktestConfigChange}
          activeStrategy={activeStrategy}
          snapshot={backtestSnapshot}
          position={backtestPosition}
          performance={backtestPerformance}
          results={backtestResults}
          speeds={speeds}
          analysisView={analysisView}
          onAnalysisViewChange={setAnalysisView}
          onStart={startBacktest}
          onStop={stopBacktest}
          busy={backtestBusy}
          loading={loading}
        />
      )}
    </main>
  );
}

function WorkspaceTabs({ activeMode, onChange, liveRunning, backtestStatus }) {
  return (
    <div className="quant-tabs">
      <button type="button" className={activeMode === 'backtest' ? 'is-active' : ''} onClick={() => onChange('backtest')}>
        <span>Backtest</span>
        <small>{humanizeStatus(backtestStatus)}</small>
      </button>
      <button type="button" className={activeMode === 'live' ? 'is-active' : ''} onClick={() => onChange('live')}>
        <span>Live paper</span>
        <small>{liveRunning ? 'Running' : 'Stopped'}</small>
      </button>
    </div>
  );
}

function LiveWorkspace({
  strategyOptions,
  selection,
  onSelectionChange,
  onUpload,
  uploadBusy,
  limits,
  settings,
  onNumberChange,
  onToggleChange,
  snapshot,
  activeStrategy,
  livePosition,
  livePerformance,
  onStart,
  onStop,
  busy,
  loading
}) {
  const isRunning = snapshot?.status === 'running';

  return (
    <section className="quant-mode-grid">
      <div className="quant-stack">
        <section className="quant-card quant-card-hero">
          <div className="quant-card-header">
            <div>
              <h3>Live paper strategy</h3>
              <span>Real-time paper execution controls with the active source visible at all times.</span>
            </div>
            <StatusBadge active={isRunning}>{isRunning ? 'Running' : 'Stopped'}</StatusBadge>
          </div>

          <StrategySourcePanel
            title="Strategy source"
            description="Choose a built-in strategy or upload a JSON strategy file. The same source can drive live and backtest modes."
            options={strategyOptions}
            selection={selection}
            onSelectionChange={onSelectionChange}
            onUpload={onUpload}
            busy={uploadBusy || isRunning || busy}
            activeStrategy={activeStrategy}
          />

          <div className="quant-control-grid">
            <Field label="Order size">
              <input type="number" min={limits.orderSizeMin} max={limits.orderSizeMax} step={limits.orderSizeStep} value={settings.orderSize} onChange={onNumberChange('orderSize')} disabled={isRunning || busy} />
              <small>{`${limits.orderSizeMin.toFixed(4)} to ${limits.orderSizeMax.toFixed(4)} BTC`}</small>
            </Field>
            <Field label="Stop loss %">
              <input type="number" min="0.01" max="25" step="0.01" value={settings.stopLossPct} onChange={onNumberChange('stopLossPct')} disabled={isRunning || busy} />
            </Field>
            <Field label="Take profit %">
              <input type="number" min="0.01" max="25" step="0.01" value={settings.takeProfitPct} onChange={onNumberChange('takeProfitPct')} disabled={isRunning || busy} />
            </Field>
            <ToggleField label="Enable long trades" checked={settings.enableLong} onChange={onToggleChange('enableLong')} disabled={isRunning || busy} />
            <ToggleField label="Enable short trades" checked={settings.enableShort} onChange={onToggleChange('enableShort')} disabled={isRunning || busy} />
            <div className="quant-action-row">
              <button className="quant-button quant-button-primary" onClick={onStart} disabled={isRunning || busy || loading || !activeStrategy}>Start live</button>
              <button className="quant-button" onClick={onStop} disabled={!isRunning || busy}>Stop</button>
            </div>
          </div>
        </section>

        <section className="quant-card">
          <div className="quant-card-header">
            <div>
              <h3>Live strategy chart</h3>
              <span>{snapshot?.strategy?.timeframe || activeStrategy?.timeframe || '1m'} · paper execution markers</span>
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
              <span>{livePosition.state}</span>
            </div>
          </div>
          <MetricGrid items={[
            ['Position state', livePosition.state],
            ['Position size', formatQty(livePosition.size)],
            ['Entry price', formatPrice(livePosition.entryPrice)],
            ['Current mark', formatPrice(livePosition.currentMarkPrice)],
            ['Notional exposure', formatMoney(livePosition.notionalExposure)],
            ['Unrealized PnL', formatMoney(livePosition.unrealizedPnl)],
            ['Realized PnL', formatMoney(livePerformance.cumulativeRealizedPnl)],
            ['Total PnL', formatMoney(livePerformance.totalPnl)],
            ['Last action', snapshot?.lastAction || 'Stopped'],
            ['Strategy status', snapshot?.strategyStatus || 'Stopped']
          ]} />
        </section>

        <section className="quant-card">
          <div className="quant-card-header">
            <div>
              <h3>Strategy context</h3>
              <span>{activeStrategy?.name || snapshot?.strategy?.name || 'Strategy'}</span>
            </div>
          </div>
          <div className="quant-rule-list">
            <RuleBlock label="Description" value={activeStrategy?.description || snapshot?.strategy?.description || 'Strategy description unavailable.'} />
            <RuleBlock label="Long entry" value={readRule(activeStrategy?.entryRules?.long || snapshot?.strategy?.entryRules?.long)} />
            <RuleBlock label="Short entry" value={readRule(activeStrategy?.entryRules?.short || snapshot?.strategy?.entryRules?.short)} />
            <RuleBlock label="Long exit" value={readRule(activeStrategy?.exitRules?.long || snapshot?.strategy?.exitRules?.long)} />
            <RuleBlock label="Short exit" value={readRule(activeStrategy?.exitRules?.short || snapshot?.strategy?.exitRules?.short)} />
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
            ['Total trades', livePerformance.totalTrades],
            ['Wins', livePerformance.wins],
            ['Losses', livePerformance.losses],
            ['Win rate', `${formatNumber(livePerformance.winRate)}%`],
            ['Best trade', formatMoney(livePerformance.bestTrade)],
            ['Worst trade', formatMoney(livePerformance.worstTrade)],
            ['Average trade', formatMoney(livePerformance.averageTrade)],
            ['Cumulative realized', formatMoney(livePerformance.cumulativeRealizedPnl)],
            ['Cumulative unrealized', formatMoney(livePerformance.cumulativeUnrealizedPnl)],
            ['Total return', `${formatNumber(livePerformance.totalReturn)}%`]
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
  );
}

function BacktestWorkspace({
  strategyOptions,
  selection,
  onSelectionChange,
  onUpload,
  uploadBusy,
  limits,
  settings,
  onNumberChange,
  onToggleChange,
  backtestConfig,
  onBacktestConfigChange,
  activeStrategy,
  snapshot,
  position,
  performance,
  results,
  speeds,
  analysisView,
  onAnalysisViewChange,
  onStart,
  onStop,
  busy,
  loading
}) {
  const status = snapshot?.status || 'idle';
  const simulationMode = status === 'running' || status === 'preparing';
  const resultsMode = status === 'completed' || status === 'cancelled' || status === 'failed';

  return (
    <section className="quant-stack quant-backtest-layout">
      <section className="quant-card quant-card-hero">
        <div className="quant-card-header">
          <div>
            <h3>Backtest control center</h3>
            <span>Fresh day-by-day historical simulation that mirrors the live paper engine wherever possible.</span>
          </div>
          <StatusBadge active={simulationMode}>{simulationMode ? 'Simulation running' : humanizeStatus(status)}</StatusBadge>
        </div>

        <div className="quant-backtest-control-grid">
          <StrategySourcePanel
            title="Strategy source"
            description="Select a built-in strategy immediately or upload a JSON strategy. Both paths are supported in the new backtest workflow."
            options={strategyOptions}
            selection={selection}
            onSelectionChange={onSelectionChange}
            onUpload={onUpload}
            busy={uploadBusy || busy || simulationMode}
            activeStrategy={activeStrategy}
          />

          <div className="quant-backtest-controls">
            <Field label="Built-in symbol">
              <input type="text" value={activeStrategy?.symbol || 'BTCUSDT'} disabled />
            </Field>
            <Field label="Start date (UTC)">
              <input type="date" value={backtestConfig.startDate} onChange={onBacktestConfigChange('startDate')} disabled={simulationMode || busy} />
            </Field>
            <Field label="End date (UTC)">
              <input type="date" value={backtestConfig.endDate} onChange={onBacktestConfigChange('endDate')} disabled={simulationMode || busy} />
            </Field>
            <Field label="Speed">
              <select value={backtestConfig.speed} onChange={onBacktestConfigChange('speed')} disabled={simulationMode || busy}>
                {(speeds || DEFAULT_SPEEDS).map((speed) => <option key={speed.key} value={speed.key}>{speed.label}</option>)}
              </select>
            </Field>
            <Field label="Order size">
              <input type="number" min={limits.orderSizeMin} max={limits.orderSizeMax} step={limits.orderSizeStep} value={settings.orderSize} onChange={onNumberChange('orderSize')} disabled={simulationMode || busy} />
              <small>{`${limits.orderSizeMin.toFixed(4)} to ${limits.orderSizeMax.toFixed(4)} BTC`}</small>
            </Field>
            <Field label="Stop loss %">
              <input type="number" min="0.01" max="25" step="0.01" value={settings.stopLossPct} onChange={onNumberChange('stopLossPct')} disabled={simulationMode || busy} />
            </Field>
            <Field label="Take profit %">
              <input type="number" min="0.01" max="25" step="0.01" value={settings.takeProfitPct} onChange={onNumberChange('takeProfitPct')} disabled={simulationMode || busy} />
            </Field>
            <ToggleField label="Longs enabled" checked={settings.enableLong} onChange={onToggleChange('enableLong')} disabled={simulationMode || busy} />
            <ToggleField label="Shorts enabled" checked={settings.enableShort} onChange={onToggleChange('enableShort')} disabled={simulationMode || busy} />
            <div className="quant-action-row quant-backtest-actions">
              <button className="quant-button quant-button-primary" onClick={onStart} disabled={simulationMode || busy || loading || !activeStrategy}>Start backtest</button>
              <button className="quant-button" onClick={onStop} disabled={!simulationMode || busy}>Stop / Cancel</button>
            </div>
          </div>
        </div>
      </section>

      {simulationMode ? (
        <div className="quant-mode-grid quant-backtest-grid">
          <section className="quant-card">
            <div className="quant-card-header">
              <div>
                <h3>Simulated market replay</h3>
                <span>{snapshot?.currentDay?.label || 'Waiting for first day'} · progressive indicators and execution markers</span>
              </div>
            </div>
            <BacktestReplayChart chart={snapshot?.chart} currentTime={snapshot?.currentDay?.currentTime} />
          </section>

          <div className="quant-stack">
            <section className="quant-card">
              <div className="quant-card-header">
                <div>
                  <h3>Current simulation status</h3>
                  <span>Live-style backtest context</span>
                </div>
              </div>
              <MetricGrid items={[
                ['Current day', snapshot?.currentDay?.label || '—'],
                ['Simulated time', formatTimestamp(snapshot?.currentDay?.currentTime)],
                ['Strategy name', snapshot?.statusPanel?.strategyName || '—'],
                ['Position state', position.state],
                ['Position size', formatQty(position.size)],
                ['Entry price', formatPrice(position.entryPrice)],
                ['Current mark', formatPrice(position.currentMarkPrice)],
                ['Unrealized PnL', formatMoney(position.unrealizedPnl)],
                ['Realized PnL', formatMoney(performance.cumulativeRealizedPnl)],
                ['Trade count', performance.totalTrades],
                ['Last action', snapshot?.statusPanel?.lastAction || '—'],
                ['Simulation status', snapshot?.statusPanel?.simulationStatus || '—']
              ]} />
              <div className="quant-inline-note">{snapshot?.statusPanel?.lastSignalReason || 'Waiting for simulation updates.'}</div>
            </section>

            <section className="quant-card">
              <div className="quant-card-header">
                <div>
                  <h3>Progress</h3>
                  <span>Two-level progress across the day and the full historical run</span>
                </div>
              </div>
              <ProgressBars snapshot={snapshot} />
            </section>
          </div>
        </div>
      ) : null}

      {resultsMode ? (
        <ResultsView snapshot={snapshot} results={results} analysisView={analysisView} onAnalysisViewChange={onAnalysisViewChange} />
      ) : null}

      {!simulationMode && !resultsMode ? (
        <section className="quant-card quant-backtest-empty">
          <div className="quant-empty-state">
            <h3>Ready for a fresh historical replay</h3>
            <p>
              Choose a strategy source, set a UTC date range, and start the backtest. The replay will process one day at a time,
              reset session-based indicators at each UTC midnight, and preserve full-period performance across the entire run.
            </p>
          </div>
        </section>
      ) : null}
    </section>
  );
}

function ResultsView({ snapshot, results, analysisView, onAnalysisViewChange }) {
  const summary = results?.summary || buildEmptyResults().summary;
  const trades = results?.trades || [];
  const analyses = results?.analyses || buildEmptyResults().analyses;

  return (
    <section className="quant-stack quant-results-stack">
      <section className="quant-card">
        <div className="quant-card-header">
          <div>
            <h3>Backtest results</h3>
            <span>{snapshot?.status === 'cancelled' ? 'Partial results from a cancelled replay.' : 'Final analytics from the completed replay.'}</span>
          </div>
          <div className="quant-header-tags">
            <span className="quant-pill">{snapshot?.strategy?.name || 'Strategy'}</span>
            <span className="quant-pill">{snapshot?.controls?.startDate} → {snapshot?.controls?.endDate}</span>
          </div>
        </div>

        <ResultsSummaryCards summary={summary} />
      </section>

      <section className="quant-card">
        <div className="quant-card-header">
          <div>
            <h3>Cumulative realized PnL</h3>
            <span>Updated after every closed trade</span>
          </div>
        </div>
        <ResultsPnlChart series={results?.cumulativePnlSeries || []} />
      </section>

      <section className="quant-card">
        <div className="quant-card-header">
          <div>
            <h3>Analysis views</h3>
            <span>Interactive charts for trade timing, outcomes, and duration behavior</span>
          </div>
          <div className="quant-analysis-tabs">
            {ANALYSIS_VIEWS.map((view) => (
              <button key={view.key} type="button" className={analysisView === view.key ? 'is-active' : ''} onClick={() => onAnalysisViewChange(view.key)}>
                {view.label}
              </button>
            ))}
          </div>
        </div>
        <AnalysisView analysisView={analysisView} analyses={analyses} />
      </section>

      <section className="quant-card">
        <div className="quant-card-header">
          <div>
            <h3>Trade log</h3>
            <span>{trades.length} closed trades recorded across the selected period</span>
          </div>
        </div>
        <BacktestTradeLog trades={trades} />
      </section>

      {results?.daySummaries?.length ? (
        <section className="quant-card">
          <div className="quant-card-header">
            <div>
              <h3>Day-by-day summary</h3>
              <span>Useful session rollup preserved across the full backtest period</span>
            </div>
          </div>
          <DaySummaryTable rows={results.daySummaries} />
        </section>
      ) : null}
    </section>
  );
}

function ResultsSummaryCards({ summary }) {
  return (
    <div className="quant-summary-grid">
      {[
        ['Total trades', summary.totalTrades],
        ['Win rate', `${formatNumber(summary.winRate)}%`],
        ['Realized PnL', formatMoney(summary.realizedPnl)],
        ['Best trade', formatMoney(summary.bestTrade)],
        ['Worst trade', formatMoney(summary.worstTrade)],
        ['Average trade', formatMoney(summary.averageTradePnl)],
        ['Avg. duration', formatDurationMinutes(summary.averageTradeDurationMinutes)],
        ['Max drawdown', `${formatNumber(summary.maxDrawdown)}%`],
        ['Profit factor', formatNumber(summary.profitFactor)],
        ['Expectancy', formatMoney(summary.expectancy)]
      ].map(([label, value]) => (
        <article key={label} className="quant-summary-card">
          <label>{label}</label>
          <strong>{value}</strong>
        </article>
      ))}
    </div>
  );
}

function ProgressBars({ snapshot }) {
  const progress = snapshot?.progress || {};

  return (
    <div className="quant-progress-stack">
      <ProgressRow label="Current day" value={progress.currentDayPercent || 0} meta={`${progress.processedCandles || 0} / ${snapshot?.currentDay?.totalCandles || 0} candles`} />
      <ProgressRow label="Full backtest" value={progress.overallPercent || 0} meta={`${snapshot?.currentDay?.index || 0} / ${progress.totalDays || 0} days`} />
      <div className="quant-progress-meta-grid">
        <div><label>Trades so far</label><strong>{progress.tradeCount || 0}</strong></div>
        <div><label>Elapsed</label><strong>{formatElapsed(progress.elapsedMs)}</strong></div>
        <div><label>Current day</label><strong>{snapshot?.currentDay?.date || '—'}</strong></div>
        <div><label>Status</label><strong>{snapshot?.statusPanel?.simulationStatus || '—'}</strong></div>
      </div>
    </div>
  );
}

function ProgressRow({ label, value, meta }) {
  return (
    <div className="quant-progress-row">
      <div className="quant-progress-head">
        <span>{label}</span>
        <strong>{formatNumber(value)}%</strong>
      </div>
      <div className="quant-progress-bar"><div style={{ width: `${Math.max(0, Math.min(100, value))}%` }} /></div>
      <small>{meta}</small>
    </div>
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
        <span>Or click to browse. Uploaded strategies become selectable immediately.</span>
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

function BacktestTradeLog({ trades }) {
  if (!trades.length) {
    return <p className="quant-empty">No closed trades were recorded for this run.</p>;
  }

  return (
    <div className="quant-table-wrap">
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
          {trades.map((trade) => (
            <tr key={`${trade.entryTime}-${trade.exitTime}-${trade.side}`}>
              <td>{formatTimestamp(trade.entryTime)}</td>
              <td>{formatTimestamp(trade.exitTime)}</td>
              <td>{trade.side?.toUpperCase() || '—'}</td>
              <td>{formatQty(trade.quantity)}</td>
              <td>{formatPrice(trade.entryPrice)}</td>
              <td>{formatPrice(trade.exitPrice)}</td>
              <td>{formatMoney(trade.realizedPnl)}</td>
              <td>{formatDurationMinutes(trade.durationMinutes)}</td>
              <td>{humanizeExitReason(trade.exitReason)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DaySummaryTable({ rows }) {
  return (
    <div className="quant-table-wrap">
      <table className="quant-table">
        <thead>
          <tr>
            <th>UTC day</th>
            <th>Trades</th>
            <th>Wins</th>
            <th>Losses</th>
            <th>Realized PnL</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.date}>
              <td>{row.date}</td>
              <td>{row.tradeCount}</td>
              <td>{row.wins}</td>
              <td>{row.losses}</td>
              <td>{formatMoney(row.realizedPnl)}</td>
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

  return <CandlestickChartSvg candles={candles} markers={chart?.markers || []} ariaLabel="Live strategy candlestick chart" showCvd={false} />;
}

function BacktestReplayChart({ chart, currentTime }) {
  const candles = chart?.candles || [];
  if (!candles.length) {
    return <p className="quant-empty">Waiting for historical candles to build the replay chart.</p>;
  }

  return (
    <div className="quant-mini-chart">
      <CandlestickChartSvg candles={candles} markers={chart?.markers || []} ariaLabel="Backtest simulation candlestick chart" showCvd />
      <div className="quant-chart-legend">
        <span><i className="is-buy" /> Buy entry</span>
        <span><i className="is-sell" /> Sell entry</span>
        <span><i className="is-exit" /> Exit</span>
        <span><i className="is-vwap" /> Session VWAP</span>
        <span><i className="is-cvd" /> Session CVD</span>
        <span>{currentTime ? `Sim time ${formatTimestamp(currentTime)}` : 'Simulation warming up'}</span>
      </div>
    </div>
  );
}

function CandlestickChartSvg({ candles, markers, ariaLabel, showCvd }) {
  const width = 980;
  const height = showCvd ? 360 : 300;
  const padding = { top: 16, right: 16, bottom: 28, left: 16 };
  const priceHeight = showCvd ? 230 : 256;
  const cvdTop = showCvd ? 268 : 0;
  const cvdHeight = showCvd ? 68 : 0;
  const minPrice = Math.min(...candles.map((candle) => candle.low), ...(markers || []).map((marker) => marker.price || Number.POSITIVE_INFINITY));
  const maxPrice = Math.max(...candles.map((candle) => candle.high), ...(markers || []).map((marker) => marker.price || Number.NEGATIVE_INFINITY));
  const plotWidth = width - padding.left - padding.right;
  const candleStep = plotWidth / Math.max(candles.length, 1);
  const bodyWidth = Math.max(candleStep * 0.56, 2);
  const priceRange = Math.max(maxPrice - minPrice, 1);
  const cvdValues = candles.map((candle) => candle.cvd_close ?? 0);
  const minCvd = Math.min(...cvdValues, 0);
  const maxCvd = Math.max(...cvdValues, 1);
  const cvdRange = Math.max(maxCvd - minCvd, 1);

  const xForIndex = (index) => padding.left + candleStep * index + candleStep / 2;
  const yForPrice = (price) => padding.top + ((maxPrice - price) / priceRange) * priceHeight;
  const yForCvd = (value) => cvdTop + ((maxCvd - value) / cvdRange) * cvdHeight;
  const vwapPath = candles.map((candle, index) => `${index === 0 ? 'M' : 'L'} ${xForIndex(index)} ${yForPrice(candle.vwap || candle.close)}`).join(' ');
  const cvdPath = showCvd ? candles.map((candle, index) => `${index === 0 ? 'M' : 'L'} ${xForIndex(index)} ${yForCvd(candle.cvd_close ?? 0)}`).join(' ') : '';
  const markerMap = new Map(candles.map((candle, index) => [candle.time, { candle, index }]));

  return (
    <div className="quant-mini-chart">
      <svg viewBox={`0 0 ${width} ${height}`} className="quant-chart-svg" role="img" aria-label={ariaLabel}>
        <rect x="0" y="0" width={width} height={height} fill="#07101c" />
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#17243a" strokeWidth="1" />
        <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#17243a" strokeWidth="1" />
        {showCvd ? <line x1={padding.left} y1={258} x2={width - padding.right} y2={258} stroke="#17243a" strokeDasharray="4 5" strokeWidth="1" /> : null}
        <path d={vwapPath} fill="none" stroke="#89aef7" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        {showCvd ? <path d={cvdPath} fill="none" stroke="#f2c96d" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /> : null}

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

        {(markers || []).map((marker, index) => {
          const mapped = markerMap.get(marker.time);
          if (!mapped) return null;
          const x = xForIndex(mapped.index);
          if (marker.action === 'BUY') {
            const y = yForPrice(marker.price || mapped.candle.low) + 10;
            return <path key={`${marker.time}-${index}`} d={`M ${x} ${y - 16} L ${x - 7} ${y - 2} L ${x + 7} ${y - 2} Z`} fill="#47c28f" />;
          }
          if (marker.action === 'SELL') {
            const y = yForPrice(marker.price || mapped.candle.high) - 10;
            return <path key={`${marker.time}-${index}`} d={`M ${x} ${y + 16} L ${x - 7} ${y + 2} L ${x + 7} ${y + 2} Z`} fill="#e16a74" />;
          }
          const y = yForPrice(marker.price || mapped.candle.close);
          return <circle key={`${marker.time}-${index}`} cx={x} cy={y} r="5" fill="#d8b04d" stroke="#07101c" strokeWidth="2" />;
        })}
      </svg>
    </div>
  );
}

function ResultsPnlChart({ series }) {
  if (!series.length) {
    return <p className="quant-empty">Cumulative realized PnL will appear here after closed trades are recorded.</p>;
  }

  const width = 980;
  const height = 260;
  const padding = { top: 18, right: 16, bottom: 26, left: 24 };
  const min = Math.min(...series.map((point) => point.cumulativeRealizedPnl), 0);
  const max = Math.max(...series.map((point) => point.cumulativeRealizedPnl), 0.01);
  const range = Math.max(max - min, 1);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const xForIndex = (index) => padding.left + (index / Math.max(series.length - 1, 1)) * plotWidth;
  const yForValue = (value) => padding.top + ((max - value) / range) * plotHeight;
  const path = series.map((point, index) => `${index === 0 ? 'M' : 'L'} ${xForIndex(index)} ${yForValue(point.cumulativeRealizedPnl)}`).join(' ');
  const areaPath = `${path} L ${xForIndex(series.length - 1)} ${height - padding.bottom} L ${xForIndex(0)} ${height - padding.bottom} Z`;
  const latest = series.at(-1)?.cumulativeRealizedPnl || 0;

  return (
    <div className="quant-results-chart">
      <svg viewBox={`0 0 ${width} ${height}`} className="quant-chart-svg" role="img" aria-label="Cumulative realized PnL chart">
        <rect x="0" y="0" width={width} height={height} fill="#07101c" />
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} stroke="#17243a" />
        <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} stroke="#17243a" />
        <path d={areaPath} fill="rgba(77, 139, 255, 0.14)" />
        <path d={path} fill="none" stroke={latest >= 0 ? '#47c28f' : '#e16a74'} strokeWidth="2.4" strokeLinejoin="round" strokeLinecap="round" />
      </svg>
      <div className="quant-chart-caption">Final cumulative realized PnL: <strong>{formatMoney(latest)}</strong></div>
    </div>
  );
}

function AnalysisView({ analysisView, analyses }) {
  if (analysisView === 'outcome') {
    return <OutcomeBreakdown outcome={analyses.outcome} exitReasons={analyses.exitReasons || []} />;
  }
  if (analysisView === 'duration') {
    return <DurationAnalysis buckets={analyses.durationBuckets || []} />;
  }
  return <TimeOfDayAnalysis rows={analyses.timeOfDay || []} />;
}

function TimeOfDayAnalysis({ rows }) {
  if (!rows.length) {
    return <p className="quant-empty">Time-of-day analysis will populate after trades are grouped into hourly buckets.</p>;
  }

  const maxTradeCount = Math.max(...rows.map((row) => row.tradeCount), 1);

  return (
    <div className="quant-analysis-grid">
      <div className="quant-bars-24">
        {rows.map((row) => (
          <article key={row.hour} className="quant-hour-bar">
            <div className="quant-hour-visual">
              <div className="quant-hour-fill" style={{ height: `${Math.max(4, row.winRate)}%`, opacity: row.tradeCount ? 1 : 0.28 }} />
            </div>
            <strong>{formatNumber(row.winRate)}%</strong>
            <span>{row.label.slice(0, 5)}</span>
            <small>{row.tradeCount} trades · avg {formatMoney(row.averagePnl)}</small>
            <div className="quant-hour-trade-fill" style={{ width: `${(row.tradeCount / maxTradeCount) * 100}%` }} />
          </article>
        ))}
      </div>
      <p className="quant-inline-note">Win rate is grouped by the UTC hour when each trade opened, using 1-hour buckets across the selected period.</p>
    </div>
  );
}

function OutcomeBreakdown({ outcome, exitReasons }) {
  const entries = [
    { label: 'Winning', value: outcome?.winning || 0, color: '#47c28f' },
    { label: 'Losing', value: outcome?.losing || 0, color: '#e16a74' },
    { label: 'Breakeven', value: outcome?.breakeven || 0, color: '#89aef7' }
  ];
  const total = entries.reduce((sum, entry) => sum + entry.value, 0);

  return (
    <div className="quant-analysis-split">
      <div className="quant-outcome-stack">
        {entries.map((entry) => (
          <div key={entry.label} className="quant-outcome-row">
            <label><i style={{ background: entry.color }} /> {entry.label}</label>
            <strong>{entry.value}</strong>
            <small>{total ? formatNumber((entry.value / total) * 100) : '0.00'}%</small>
          </div>
        ))}
      </div>
      <div className="quant-exit-reasons">
        <h4>Exit reason mix</h4>
        {(exitReasons || []).length ? exitReasons.map((reason) => (
          <div key={reason.reason} className="quant-exit-row">
            <span>{humanizeExitReason(reason.reason)}</span>
            <strong>{reason.count}</strong>
          </div>
        )) : <p className="quant-empty">No exit reasons recorded yet.</p>}
      </div>
    </div>
  );
}

function DurationAnalysis({ buckets }) {
  if (!buckets.length) {
    return <p className="quant-empty">Trade duration analysis will appear after the backtest closes trades.</p>;
  }

  const maxCount = Math.max(...buckets.map((bucket) => bucket.count), 1);

  return (
    <div className="quant-duration-bars">
      {buckets.map((bucket) => (
        <article key={bucket.key} className="quant-duration-card">
          <div className="quant-duration-bar"><div style={{ width: `${(bucket.count / maxCount) * 100}%` }} /></div>
          <div className="quant-duration-meta">
            <strong>{bucket.key}</strong>
            <span>{bucket.count} trades</span>
          </div>
        </article>
      ))}
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
    averageTradeDurationMinutes: 0,
    cumulativeRealizedPnl: 0,
    cumulativeUnrealizedPnl: 0,
    totalPnl: 0,
    totalReturn: 0,
    profitFactor: 0,
    expectancy: 0,
    maxDrawdown: 0
  };
}

function buildEmptyResults() {
  return {
    summary: {
      totalTrades: 0,
      wins: 0,
      losses: 0,
      winRate: 0,
      realizedPnl: 0,
      bestTrade: 0,
      worstTrade: 0,
      averageTradePnl: 0,
      averageTradeDurationMinutes: 0,
      maxDrawdown: 0,
      profitFactor: 0,
      expectancy: 0
    },
    cumulativePnlSeries: [],
    trades: [],
    analyses: {
      timeOfDay: [],
      outcome: { winning: 0, losing: 0, breakeven: 0 },
      durationBuckets: [],
      exitReasons: [],
      sessions: []
    },
    daySummaries: []
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

function buildDefaultBacktestConfig() {
  const end = new Date();
  end.setUTCDate(end.getUTCDate() - 1);
  const start = new Date(end);
  start.setUTCDate(end.getUTCDate() - 4);
  return {
    startDate: start.toISOString().slice(0, 10),
    endDate: end.toISOString().slice(0, 10),
    speed: 'fast'
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
    max_holding_bars: 'Max holding bars',
    'Long signal confirmed.': 'Long signal confirmed',
    'Short signal confirmed.': 'Short signal confirmed'
  };
  return labels[value] || value || '—';
}

function humanizeStatus(value) {
  const labels = {
    idle: 'Idle',
    preparing: 'Preparing',
    running: 'Running',
    completed: 'Completed',
    cancelled: 'Cancelled',
    failed: 'Failed'
  };
  return labels[value] || 'Idle';
}

function formatTimestamp(value) {
  if (!value) return '—';
  return new Date(value).toLocaleString(undefined, {
    hour12: false,
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  });
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

function formatElapsed(value) {
  const totalSeconds = Math.max(0, Math.floor(Number(value || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
}

function formatDurationMinutes(value) {
  if (!Number.isFinite(Number(value))) return '—';
  const minutes = Number(value || 0);
  if (minutes < 60) return `${formatNumber(minutes)}m`;
  const hours = Math.floor(minutes / 60);
  const rem = minutes % 60;
  return `${hours}h ${formatNumber(rem)}m`;
}

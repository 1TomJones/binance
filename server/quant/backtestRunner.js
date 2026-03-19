import { enrichMarketCandles } from './candleEnrichment.js';
import { StrategyExecutionEngine } from './strategyExecutionEngine.js';

const DEFAULT_SPEED = 'fast';
const CHART_WINDOW = 180;

export const BACKTEST_SPEEDS = {
  step: { key: 'step', label: 'Step', candlesPerChunk: 1, delayMs: 120 },
  steady: { key: 'steady', label: 'Steady', candlesPerChunk: 6, delayMs: 80 },
  fast: { key: 'fast', label: 'Fast', candlesPerChunk: 30, delayMs: 18 },
  turbo: { key: 'turbo', label: 'Turbo', candlesPerChunk: 180, delayMs: 0 }
};

export class BacktestRunner {
  constructor({ strategyResolver, executionEngine = new StrategyExecutionEngine(), fetchDayCandles }) {
    this.strategyResolver = strategyResolver;
    this.executionEngine = executionEngine;
    this.fetchDayCandles = fetchDayCandles;
    this.active = null;
    this.runCounter = 0;
  }

  start({ strategyRef, runConfig = {}, startDate, endDate, speed = DEFAULT_SPEED }) {
    const resolved = this.strategyResolver(strategyRef);
    if (!resolved) {
      throw new Error('Unknown strategy selection.');
    }

    const dates = enumerateUtcDates(startDate, endDate);
    if (!dates.length) {
      throw new Error('Select a valid UTC date range.');
    }

    const speedProfile = BACKTEST_SPEEDS[speed] || BACKTEST_SPEEDS[DEFAULT_SPEED];
    const engineState = this.executionEngine.createRunState({
      strategy: resolved.strategy,
      runConfig
    });

    const runId = ++this.runCounter;
    this.active = {
      runId,
      status: 'preparing',
      phase: 'simulation',
      startedAt: Date.now(),
      finishedAt: null,
      error: null,
      cancelRequested: false,
      strategyRef,
      strategy: resolved.strategy,
      strategySummary: resolved.summary,
      symbol: resolved.strategy.market.symbol,
      timeframe: resolved.strategy.market.timeframe,
      runConfig: {
        orderSize: engineState.orderSize,
        stopLossPct: engineState.settings.stopLossPct,
        takeProfitPct: engineState.settings.takeProfitPct,
        enableLong: engineState.settings.enableLong,
        enableShort: engineState.settings.enableShort,
        speed: speedProfile.key
      },
      speedProfile,
      engineState,
      dateRange: { startDate, endDate },
      days: dates,
      currentDayIndex: -1,
      currentDay: null,
      currentTime: null,
      lastPrice: null,
      totalCandles: dates.length * 1440,
      processedCandles: 0,
      completedDays: 0,
      chartCandles: [],
      chartMarkers: [],
      lastAction: 'Preparing',
      lastSignalReason: 'Loading historical data.',
      strategyStatus: 'Preparing historical replay',
      daySummaries: [],
      finalized: null
    };

    void this.#run(runId);
    return this.getSnapshot();
  }

  stop() {
    if (!this.active) return this.getSnapshot();
    this.active.cancelRequested = true;
    if (this.active.status === 'running' || this.active.status === 'preparing') {
      this.active.strategyStatus = 'Stopping backtest';
      this.active.lastAction = 'Cancel requested';
      this.active.lastSignalReason = 'Cancelling after the current simulation batch.';
    }
    return this.getSnapshot();
  }

  getSnapshot() {
    if (!this.active) {
      return buildIdleSnapshot();
    }

    const active = this.active;
    const finalized = active.finalized || this.executionEngine.finalizeRun({
      strategy: active.strategy,
      state: active.engineState,
      lastPrice: active.lastPrice
    });
    const metrics = finalized.metrics;

    return {
      status: active.status,
      phase: active.phase,
      error: active.error,
      startedAt: active.startedAt,
      finishedAt: active.finishedAt,
      elapsedMs: (active.finishedAt || Date.now()) - active.startedAt,
      strategy: {
        key: active.strategySummary.id,
        name: active.strategySummary.name,
        description: active.strategySummary.description,
        timeframe: active.timeframe,
        symbol: active.symbol,
        source: active.strategySummary.source,
        entryRules: active.strategySummary.entryRules,
        exitRules: active.strategySummary.exitRules
      },
      controls: {
        ...active.runConfig,
        startDate: active.dateRange.startDate,
        endDate: active.dateRange.endDate
      },
      currentDay: active.currentDay ? {
        date: active.currentDay.date,
        label: active.currentDay.label,
        index: active.currentDayIndex + 1,
        total: active.days.length,
        processedCandles: active.currentDay.processedCandles,
        totalCandles: active.currentDay.totalCandles,
        currentTime: active.currentTime,
        startTime: active.currentDay.startTime,
        endTime: active.currentDay.endTime
      } : null,
      progress: {
        overallPercent: round(active.days.length ? (((active.completedDays || 0) + ((active.currentDay?.totalCandles ? active.currentDay.processedCandles / active.currentDay.totalCandles : 0))) / active.days.length) * 100 : active.status === 'completed' ? 100 : 0),
        currentDayPercent: round(active.currentDay?.totalCandles ? (active.currentDay.processedCandles / active.currentDay.totalCandles) * 100 : 0),
        processedDays: active.completedDays || 0,
        totalDays: active.days.length,
        processedCandles: active.processedCandles,
        totalCandles: active.totalCandles,
        tradeCount: active.engineState.trades.length,
        elapsedMs: (active.finishedAt || Date.now()) - active.startedAt
      },
      chart: {
        candles: active.chartCandles,
        markers: active.chartMarkers
      },
      statusPanel: {
        strategyName: active.strategySummary.name,
        simulationStatus: active.strategyStatus,
        lastAction: active.lastAction,
        lastSignalReason: active.lastSignalReason
      },
      position: active.engineState.position ? buildPositionView(active.engineState.position, active.lastPrice) : buildFlatPosition(active.lastPrice),
      performance: {
        totalTrades: metrics.totalTrades,
        wins: metrics.wins,
        losses: metrics.losses,
        winRate: metrics.winRate,
        bestTrade: metrics.bestTrade,
        worstTrade: metrics.worstTrade,
        averageTrade: metrics.averageTradePnl,
        averageTradeDurationMinutes: metrics.averageTradeDurationMinutes,
        cumulativeRealizedPnl: metrics.realizedPnl,
        cumulativeUnrealizedPnl: metrics.unrealizedPnl,
        totalPnl: metrics.netPnl,
        totalReturn: metrics.returnPct,
        profitFactor: metrics.profitFactor,
        expectancy: metrics.expectancy,
        maxDrawdown: metrics.maxDrawdown
      },
      results: {
        summary: metrics,
        cumulativePnlSeries: finalized.cumulativePnlSeries,
        trades: finalized.trades,
        analyses: finalized.analyses,
        daySummaries: active.daySummaries
      }
    };
  }

  async #run(runId) {
    const active = this.active;
    if (!active || active.runId !== runId) return;

    try {
      for (let dayIndex = 0; dayIndex < active.days.length; dayIndex += 1) {
        if (!this.#isActive(runId) || active.cancelRequested) break;
        await this.#runDay(active, dayIndex);
      }

      this.#finalize(active, active.cancelRequested ? 'cancelled' : 'completed');
    } catch (error) {
      active.status = 'failed';
      active.phase = 'results';
      active.error = error?.message || String(error);
      active.finishedAt = Date.now();
      active.strategyStatus = 'Backtest failed';
      active.lastAction = 'Error';
      active.lastSignalReason = active.error;
      active.finalized = this.executionEngine.finalizeRun({
        strategy: active.strategy,
        state: active.engineState,
        lastPrice: active.lastPrice
      });
    }
  }

  async #runDay(active, dayIndex) {
    const date = active.days[dayIndex];
    const dayStartMs = Date.parse(`${date}T00:00:00.000Z`);
    const dayEndMs = dayStartMs + (24 * 60 * 60 * 1000) - 1;

    active.status = 'preparing';
    active.currentDayIndex = dayIndex;
    active.currentDay = {
      date,
      label: formatUtcDate(date),
      processedCandles: 0,
      totalCandles: 0,
      startTime: dayStartMs,
      endTime: dayEndMs
    };
    active.chartCandles = [];
    active.chartMarkers = [];
    active.currentTime = dayStartMs;
    active.strategyStatus = 'Loading day';
    active.lastAction = 'Reset';
    active.lastSignalReason = 'Resetting session state at 00:00 UTC.';
    active.engineState.session = this.executionEngine.createSessionState();
    active.engineState.session.sessionDate = date;
    active.engineState.lastProcessedCandleTime = null;

    const rawCandles = await this.fetchDayCandles({
      symbol: active.symbol,
      date,
      timeframe: active.timeframe
    });

    const enrichedCandles = enrichMarketCandles(rawCandles, buildDayIndicatorMaps(rawCandles, active.runConfig));
    active.currentDay.totalCandles = enrichedCandles.length;
    active.totalCandles += Math.max(0, enrichedCandles.length - 1440);
    active.status = 'running';
    active.strategyStatus = enrichedCandles.length ? 'Replaying session' : 'No candles for selected day';

    if (!enrichedCandles.length) {
      active.daySummaries.push({ date, tradeCount: 0, wins: 0, losses: 0, realizedPnl: 0 });
      active.completedDays += 1;
      return;
    }

    const fillModel = this.executionEngine.createFillModel();
    const tradeCountBeforeDay = active.engineState.trades.length;
    let candleIndex = 0;

    while (candleIndex < enrichedCandles.length) {
      if (!this.#isActive(active.runId) || active.cancelRequested) break;

      const chunkEnd = Math.min(candleIndex + active.speedProfile.candlesPerChunk, enrichedCandles.length);
      for (let index = candleIndex; index < chunkEnd; index += 1) {
        const candle = enrichedCandles[index];
        const tradesBefore = active.engineState.trades.length;
        const previousPosition = active.engineState.position;

        this.executionEngine.processCandle({
          strategy: active.strategy,
          state: active.engineState,
          candle,
          fillModel,
          currentDateLabel: date
        });

        active.currentDay.processedCandles = index + 1;
        active.processedCandles += 1;
        active.currentTime = candle.time * 1000;
        active.lastPrice = candle.close;
        active.chartCandles = enrichedCandles.slice(Math.max(0, index + 1 - CHART_WINDOW), index + 1);

        if (!previousPosition && active.engineState.position) {
          active.lastAction = active.engineState.position.side === 'long' ? 'BUY' : 'SELL';
          active.lastSignalReason = active.engineState.position.entryReason;
          active.strategyStatus = `Position opened · ${active.engineState.position.side}`;
          active.chartMarkers.push({
            action: active.engineState.position.side === 'long' ? 'BUY' : 'SELL',
            time: candle.time,
            price: active.engineState.position.entryPrice,
            side: active.engineState.position.side
          });
        } else if (active.engineState.trades.length > tradesBefore) {
          const trade = active.engineState.trades.at(-1);
          active.lastAction = 'EXIT';
          active.lastSignalReason = formatExitReason(trade.exitReason);
          active.strategyStatus = 'Flat · monitoring next setup';
          active.chartMarkers.push({
            action: 'EXIT',
            time: candle.time,
            price: trade.exitPrice,
            side: trade.side,
            reason: trade.exitReason
          });
        } else {
          active.lastAction = active.engineState.position ? 'Holding' : 'Scan';
          active.lastSignalReason = active.engineState.position
            ? `${active.engineState.position.side === 'long' ? 'Long' : 'Short'} position remains active.`
            : 'No entry trigger on this candle.';
          active.strategyStatus = active.engineState.position ? 'Position active' : 'Scanning';
        }
      }

      candleIndex = chunkEnd;
      if (active.speedProfile.delayMs > 0) {
        await sleep(active.speedProfile.delayMs);
      } else {
        await sleep(0);
      }
    }

    if (active.cancelRequested) {
      return;
    }

    const flattened = this.executionEngine.finalizeDay({
      strategy: active.strategy,
      state: active.engineState,
      fillModel,
      dateLabel: date
    });

    if (flattened) {
      active.chartMarkers.push({
        action: 'EXIT',
        time: flattened.exitTime / 1000,
        price: flattened.exitPrice,
        side: flattened.side,
        reason: flattened.exitReason
      });
      active.lastAction = 'EXIT';
      active.lastSignalReason = 'Day complete · flattened at session close.';
      active.strategyStatus = 'Session closed';
    } else {
      active.lastAction = 'Reset';
      active.lastSignalReason = 'Day complete with no open position.';
      active.strategyStatus = 'Session closed';
    }

    const dayTrades = active.engineState.trades.slice(tradeCountBeforeDay);
    active.daySummaries.push(buildDaySummary(date, dayTrades));
    active.currentTime = dayEndMs;
    active.completedDays += 1;
  }

  #finalize(active, status) {
    active.status = status;
    active.phase = 'results';
    active.finishedAt = Date.now();
    active.strategyStatus = status === 'completed' ? 'Backtest complete' : 'Backtest cancelled';
    active.lastAction = status === 'completed' ? 'Complete' : 'Cancelled';
    active.lastSignalReason = status === 'completed'
      ? 'Historical replay finished across the full selected period.'
      : 'Backtest stopped before finishing all selected days.';
    active.finalized = this.executionEngine.finalizeRun({
      strategy: active.strategy,
      state: active.engineState,
      lastPrice: active.lastPrice
    });
  }

  #isActive(runId) {
    return this.active && this.active.runId === runId;
  }
}

function buildIdleSnapshot() {
  return {
    status: 'idle',
    phase: 'config',
    error: null,
    startedAt: null,
    finishedAt: null,
    elapsedMs: 0,
    strategy: null,
    controls: null,
    currentDay: null,
    progress: {
      overallPercent: 0,
      currentDayPercent: 0,
      processedDays: 0,
      totalDays: 0,
      processedCandles: 0,
      totalCandles: 0,
      tradeCount: 0,
      elapsedMs: 0
    },
    chart: { candles: [], markers: [] },
    statusPanel: {
      strategyName: 'No strategy selected',
      simulationStatus: 'Idle',
      lastAction: 'Waiting',
      lastSignalReason: 'Configure a date range and start the backtest.'
    },
    position: buildFlatPosition(null),
    performance: {
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
    },
    results: {
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
    }
  };
}

function buildDayIndicatorMaps(candles, settings) {
  const byBucket = new Map();
  const vwapByTime = new Map();
  const cvdByTime = new Map();

  let cumulativeVolume = 0;
  let cumulativeVolumePrice = 0;
  let runningCvd = 0;

  candles.forEach((candle) => {
    const totalVolume = Number(candle.volume || 0);
    const buyVolume = Number(candle.takerBuyBaseVolume || 0);
    const sellVolume = Math.max(0, totalVolume - buyVolume);
    byBucket.set(candle.time, { buy: buyVolume, sell: sellVolume });

    cumulativeVolume += totalVolume;
    cumulativeVolumePrice += Number(candle.close || 0) * totalVolume;
    vwapByTime.set(candle.time, cumulativeVolume ? cumulativeVolumePrice / cumulativeVolume : Number(candle.close || 0));

    const cvdOpen = runningCvd;
    runningCvd += buyVolume - sellVolume;
    cvdByTime.set(candle.time, {
      open: cvdOpen,
      high: Math.max(cvdOpen, runningCvd),
      low: Math.min(cvdOpen, runningCvd),
      close: runningCvd
    });
  });

  return {
    vwapByTime,
    cvdByTime,
    byBucket,
    settings
  };
}

function buildDaySummary(date, dayTrades) {
  const wins = dayTrades.filter((trade) => Number(trade.realizedPnl || 0) > 0).length;
  const losses = dayTrades.filter((trade) => Number(trade.realizedPnl || 0) < 0).length;
  return {
    date,
    tradeCount: dayTrades.length,
    wins,
    losses,
    realizedPnl: round(dayTrades.reduce((sum, trade) => sum + Number(trade.realizedPnl || 0), 0))
  };
}

function buildPositionView(position, markPrice) {
  const currentMarkPrice = Number(markPrice || position.entryPrice || 0);
  const unrealizedPnl = position.side === 'long'
    ? (currentMarkPrice - position.entryPrice) * position.quantity
    : (position.entryPrice - currentMarkPrice) * position.quantity;

  return {
    state: position.side === 'long' ? 'Long' : 'Short',
    side: position.side,
    size: position.quantity,
    entryPrice: position.entryPrice,
    currentMarkPrice,
    notionalExposure: round(position.quantity * currentMarkPrice),
    unrealizedPnl: round(unrealizedPnl),
    entryReason: position.entryReason
  };
}

function buildFlatPosition(markPrice) {
  return {
    state: 'Flat',
    side: null,
    size: 0,
    entryPrice: null,
    currentMarkPrice: markPrice,
    notionalExposure: 0,
    unrealizedPnl: 0,
    entryReason: null
  };
}

function enumerateUtcDates(startDate, endDate) {
  const start = Date.parse(`${startDate}T00:00:00.000Z`);
  const end = Date.parse(`${endDate}T00:00:00.000Z`);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    throw new Error('Start date must be on or before end date.');
  }

  const dates = [];
  for (let cursor = start; cursor <= end; cursor += 24 * 60 * 60 * 1000) {
    dates.push(new Date(cursor).toISOString().slice(0, 10));
    if (dates.length > 62) {
      throw new Error('Backtests are limited to 62 UTC days per run for stability.');
    }
  }
  return dates;
}

function formatUtcDate(dateString) {
  return new Date(`${dateString}T00:00:00.000Z`).toLocaleDateString('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  });
}

function formatExitReason(reason) {
  const labels = {
    stop_loss: 'Stop loss exit triggered.',
    take_profit: 'Take profit target hit.',
    signal_exit: 'Strategy exit signal confirmed.',
    end_of_day_exit: 'End of day flatten executed.',
    max_holding_bars: 'Maximum holding time reached.'
  };
  return labels[reason] || 'Position closed.';
}

function round(value) {
  return Number((value || 0).toFixed(4));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

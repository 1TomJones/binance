import { HistoricalCoverageError } from './historicalBacktestDataService.js';
import { HistoricalMarketReplay } from './historicalMarketReplay.js';
import { timeframeToSeconds } from '../sessionAnalytics.js';

const PROGRESS_EMIT_EVERY_CANDLES = 25;
const EVENT_LOOP_YIELD_EVERY_CANDLES = 100;
const EVENT_LOOP_YIELD_EVERY_TRADES = 2000;
const EVENT_LOOP_SLICE_BUDGET_MS = 12;
const FINALIZING_PROGRESS_PCT = 95;

export class BacktestRunner {
  constructor({ executionEngine, loadTrades, loadSeedTrade, historicalDataService }) {
    this.executionEngine = executionEngine;
    this.loadTrades = loadTrades;
    this.loadSeedTrade = loadSeedTrade;
    this.historicalDataService = historicalDataService;
  }

  async run({ strategy, runConfig, progressCallback, shouldStop, coveragePlan = null }) {
    const startDate = normalizeDay(runConfig.startDate);
    const endDate = normalizeDay(runConfig.endDate || runConfig.startDate);
    const replayDays = coveragePlan?.days?.length
      ? coveragePlan.days
      : [...iterateUtcDays(startDate, endDate)].map((day) => ({ ...day, targetEndMs: day.dayEndMs }));

    const runState = this.executionEngine.createRunState({ strategy, runConfig });
    const totalDays = replayDays.length;
    const timeframeSec = timeframeToSeconds(strategy.market?.timeframe || '1m');
    const estimatedCandlesPerDay = Math.max(Math.floor(86400 / timeframeSec), 1);
    const totalCandlesTarget = Math.max(totalDays * estimatedCandlesPerDay, 1);
    const sessionResults = [];
    const startedAtMs = Date.parse(runConfig.startedAtIso || new Date().toISOString());
    const fillModel = this.executionEngine.createFillModel({
      syntheticSpreadBps: runState.settings.syntheticSpreadBps
    });

    const runtime = {
      processedCandles: 0,
      processedTrades: 0,
      emittedSinceProgress: 0,
      candlesSinceYield: 0,
      tradesSinceYield: 0,
      sliceStartedAtMs: Date.now()
    };

    const emitProgress = ({
      currentDate,
      currentDay,
      stage,
      replay = null,
      hydration = null,
      progressPct = null,
      phase = 'replaying'
    }) => {
      progressCallback?.({
        processed: runtime.processedCandles,
        total: totalCandlesTarget,
        progressPct: Number.isFinite(Number(progressPct))
          ? Number(progressPct)
          : computeReplayProgressPct({ currentDay, totalDays, replayPercent: replay?.percent }),
        currentDate,
        currentDay,
        totalDays,
        totalTrades: runState.trades.length,
        elapsedMs: Date.now() - startedAtMs,
        marker: stage,
        phase,
        hydration,
        replay: replay
          ? {
              ...replay,
              processedCandles: runtime.processedCandles,
              processedTrades: runtime.processedTrades,
              totalCandlesTarget
            }
          : null
      });
    };

    for (const day of replayDays) {
      shouldStop?.();
      const { dayStartMs, dayEndMs, isoDate, dayIndex = 0, targetEndMs = day.dayEndMs } = day;

      emitProgress({
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        stage: 'Loading prepared local historical session',
        hydration: {
          source: day.coverage?.source || null,
          status: 'complete',
          rowsIngested: Number(day.coverage?.checkpoint_rows || day.coverage?.trade_count || 0),
          pagesIngested: day.coverage?.source === 'bulk-file' ? 1 : 0,
          checkpointTimeMs: day.coverage?.checkpoint_time_ms ?? targetEndMs,
          lastAggTradeId: day.coverage?.last_agg_trade_id ?? null,
          retry: null,
          percent: 100
        },
        replay: {
          day: isoDate,
          status: 'pending',
          replayedTrades: 0,
          totalTrades: 0,
          percent: computeDayProgressPct({ replayedTrades: 0, totalTrades: 0 })
        }
      });

      const dayPayload = await this.#loadReplayDay({
        strategy,
        day,
        shouldStop,
        progressCallback: (payload = {}) => {
          emitProgress({
            currentDate: isoDate,
            currentDay: dayIndex + 1,
            stage: payload.stage || 'Loading prepared local historical session',
            hydration: payload.hydration || null,
            replay: {
              day: isoDate,
              status: 'pending',
              replayedTrades: 0,
              totalTrades: Number(payload.tradeCount || 0),
              percent: computeDayProgressPct({ replayedTrades: 0, totalTrades: Number(payload.tradeCount || 0) })
            },
            progressPct: payload.progressPct,
            phase: payload.phase || 'replaying'
          });
        },
        targetEndMs
      });

      const tradeSource = dayPayload.tradeStream || dayPayload.trades || [];
      const seedTrade = dayPayload.seedTrade || null;
      const totalDayTrades = Math.max(Number(dayPayload.tradeCount ?? (Array.isArray(tradeSource) ? tradeSource.length : 0)) || 0, 1);
      const sessionReplay = new HistoricalMarketReplay({
        timeframe: strategy.market.timeframe,
        sessionStartMs: dayStartMs,
        sessionEndMs: dayEndMs,
        seedPrice: seedTrade?.price ?? null,
        settings: runState.settings
      });

      const dayTradeStartIndex = runState.trades.length;
      let replayedTradeCount = 0;
      let emittedCandleCountForDay = 0;

      for await (const trade of iterateTrades(tradeSource)) {
        shouldStop?.();
        replayedTradeCount += 1;
        runtime.processedTrades += 1;
        runtime.tradesSinceYield += 1;

        if (sessionReplay.seedPrice == null && Number.isFinite(Number(trade?.price))) {
          sessionReplay.seedPrice = Number(trade.price);
        }

        const closedCandles = sessionReplay.processTrade(trade);
        emittedCandleCountForDay += closedCandles.length;
        await this.#drainCandles({
          closedCandles,
          strategy,
          runState,
          fillModel,
          runtime,
          emitProgress,
          currentDate: isoDate,
          currentDay: dayIndex + 1,
          progressStage: `Replaying session ${Math.min(replayedTradeCount, totalDayTrades)}/${totalDayTrades} trades`,
          totalDayTrades,
          replayedTradeCount
        });

        await maybeYieldToEventLoop(runtime);
      }

      const flushedCandles = sessionReplay.flushRemaining();
      emittedCandleCountForDay += flushedCandles.length;
      await this.#drainCandles({
        closedCandles: flushedCandles,
        strategy,
        runState,
        fillModel,
        runtime,
        emitProgress,
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        progressStage: 'Flushing end-of-day candles',
        totalDayTrades,
        replayedTradeCount
      });

      const flattenedTrade = this.executionEngine.finalizeDay({
        strategy,
        state: runState,
        fillModel,
        dateLabel: isoDate
      });

      const dayTradesClosed = runState.trades.slice(dayTradeStartIndex);
      sessionResults.push(buildSessionResult({
        isoDate,
        candleCount: emittedCandleCountForDay,
        trades: dayTradesClosed,
        sourceTradeCount: replayedTradeCount,
        flattenedTrade
      }));

      emitProgress({
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        stage: flattenedTrade ? 'Session flattened at UTC close' : 'Session complete',
        hydration: {
          source: dayPayload.hydrationSource || day.coverage?.source || null,
          status: 'complete',
          rowsIngested: Number(day.coverage?.checkpoint_rows || dayPayload.tradeCount || 0),
          pagesIngested: dayPayload.hydrationSource === 'bulk-file' ? 1 : 0,
          checkpointTimeMs: day.coverage?.checkpoint_time_ms ?? targetEndMs,
          lastAggTradeId: dayPayload.lastAggTradeId || day.coverage?.last_agg_trade_id || null,
          retry: null,
          percent: 100
        },
        replay: {
          day: isoDate,
          status: 'complete',
          replayedTrades: replayedTradeCount,
          totalTrades: totalDayTrades,
          percent: 100
        }
      });

      await schedulerYield();
      runtime.sliceStartedAtMs = Date.now();
      runtime.candlesSinceYield = 0;
      runtime.tradesSinceYield = 0;
    }

    progressCallback?.({
      processed: runtime.processedCandles,
      total: totalCandlesTarget,
      progressPct: FINALIZING_PROGRESS_PCT,
      currentDate: replayDays.at(-1)?.isoDate || null,
      currentDay: totalDays || null,
      totalDays,
      totalTrades: runState.trades.length,
      elapsedMs: Date.now() - startedAtMs,
      marker: 'Finalizing results',
      phase: 'finalizing',
      hydration: null,
      replay: null
    });

    const result = this.executionEngine.finalizeRun({
      strategy,
      state: runState,
      lastPrice: runState.session.previousCandle?.close || null
    });

    return {
      ...result,
      analyses: this.executionEngine.metricsCalculator.buildAnalyses({ trades: runState.trades, sessionResults }),
      sessionResults
    };
  }

  async #loadReplayDay({ strategy, day, shouldStop, progressCallback, targetEndMs }) {
    if (this.historicalDataService?.loadPreparedDay && (day.coverage != null || day.classification != null)) {
      return this.historicalDataService.loadPreparedDay({
        symbol: strategy.market.symbol,
        dayStartMs: day.dayStartMs,
        dayEndMs: day.dayEndMs,
        targetEndMs,
        shouldStop,
        progressCallback,
        coveragePlanEntry: day
      });
    }

    if (this.historicalDataService?.loadDay) {
      return this.historicalDataService.loadDay({
        symbol: strategy.market.symbol,
        dayStartMs: day.dayStartMs,
        dayEndMs: day.dayEndMs,
        shouldStop,
        progressCallback
      });
    }

    const trades = this.loadTrades({
      symbol: strategy.market.symbol,
      startMs: day.dayStartMs,
      endMs: targetEndMs,
      limit: null
    });
    const seedTrade = this.loadSeedTrade?.({
      symbol: strategy.market.symbol,
      beforeMs: day.dayStartMs
    }) || null;

    if (!trades) {
      throw new HistoricalCoverageError(`Historical trade coverage is not ready for ${strategy.market.symbol} on ${day.isoDate}.`);
    }

    return { trades, seedTrade };
  }

  async #drainCandles({
    closedCandles,
    strategy,
    runState,
    fillModel,
    runtime,
    emitProgress,
    currentDate,
    currentDay,
    progressStage,
    totalDayTrades,
    replayedTradeCount
  }) {
    for (const candle of closedCandles) {
      this.executionEngine.processCandle({
        strategy,
        state: runState,
        candle,
        fillModel,
        currentDateLabel: currentDate
      });

      runtime.processedCandles += 1;
      runtime.emittedSinceProgress += 1;
      runtime.candlesSinceYield += 1;

      if (runtime.emittedSinceProgress >= PROGRESS_EMIT_EVERY_CANDLES) {
        emitProgress({
          currentDate,
          currentDay,
          stage: progressStage,
          replay: {
            day: currentDate,
            status: 'running',
            replayedTrades: replayedTradeCount,
            totalTrades: totalDayTrades,
            percent: computeDayProgressPct({
              replayedTrades: replayedTradeCount,
              totalTrades: totalDayTrades
            })
          }
        });
        runtime.emittedSinceProgress = 0;
      }

      await maybeYieldToEventLoop(runtime);
    }
  }
}

function computeReplayProgressPct({ currentDay, totalDays, replayPercent }) {
  const safeCurrentDay = Math.max(Number(currentDay) || 1, 1);
  const safeTotalDays = Math.max(Number(totalDays) || 1, 1);
  const completedDays = Math.min(safeCurrentDay - 1, safeTotalDays);
  const replayRatio = clampRatio((Number(replayPercent) || 0) / 100);
  const weightedRunRatio = 0.5 + (replayRatio * 0.45);
  return ((completedDays + clampRatio(weightedRunRatio)) / safeTotalDays) * 100;
}

function computeDayProgressPct({ replayedTrades, totalTrades }) {
  if (!Number.isFinite(Number(totalTrades)) || Number(totalTrades) <= 0) return 0;
  return clampRatio(Number(replayedTrades || 0) / Number(totalTrades)) * 100;
}

function clampRatio(value) {
  return Math.max(0, Math.min(1, Number(value) || 0));
}

function buildSessionResult({ isoDate, candleCount, trades, sourceTradeCount, flattenedTrade }) {
  const realizedPnl = trades.reduce((sum, trade) => sum + Number(trade.realizedPnl || 0), 0);
  const wins = trades.filter((trade) => Number(trade.realizedPnl || 0) > 0).length;
  const losses = trades.filter((trade) => Number(trade.realizedPnl || 0) < 0).length;
  const breakeven = trades.filter((trade) => Number(trade.realizedPnl || 0) === 0).length;

  return {
    date: isoDate,
    tradeCount: trades.length,
    wins,
    losses,
    breakeven,
    realizedPnl: round(realizedPnl),
    bestTrade: round(Math.max(...trades.map((trade) => Number(trade.realizedPnl || 0)), 0)),
    worstTrade: round(Math.min(...trades.map((trade) => Number(trade.realizedPnl || 0)), 0)),
    averageTradePnl: round(trades.length ? realizedPnl / trades.length : 0),
    averageDurationMinutes: round(trades.length ? trades.reduce((sum, trade) => sum + Number(trade.durationMinutes || 0), 0) / trades.length : 0),
    candleCount,
    sourceTradeCount,
    endOfDayExit: flattenedTrade?.exitReason || null
  };
}

function normalizeDay(value) {
  const date = new Date(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function* iterateUtcDays(startDate, endDate) {
  let cursor = startDate.getTime();
  let dayIndex = 0;
  while (cursor <= endDate.getTime()) {
    yield {
      dayStartMs: cursor,
      dayEndMs: cursor + 86400000 - 1,
      isoDate: new Date(cursor).toISOString().slice(0, 10),
      dayIndex
    };
    cursor += 86400000;
    dayIndex += 1;
  }
}

function round(value) {
  return Number((value || 0).toFixed(4));
}

async function maybeYieldToEventLoop(runtime) {
  const elapsedMs = Date.now() - Number(runtime.sliceStartedAtMs || 0);
  const shouldYield = runtime.candlesSinceYield >= EVENT_LOOP_YIELD_EVERY_CANDLES
    || runtime.tradesSinceYield >= EVENT_LOOP_YIELD_EVERY_TRADES
    || elapsedMs >= EVENT_LOOP_SLICE_BUDGET_MS;

  if (!shouldYield) return;

  await schedulerYield();
  runtime.sliceStartedAtMs = Date.now();
  runtime.candlesSinceYield = 0;
  runtime.tradesSinceYield = 0;
}

function schedulerYield() {
  return new Promise((resolve) => {
    if (typeof setImmediate === 'function') {
      setImmediate(resolve);
      return;
    }
    setTimeout(resolve, 0);
  });
}

async function* iterateTrades(tradeSource) {
  if (!tradeSource) return;
  if (typeof tradeSource[Symbol.asyncIterator] === 'function') {
    for await (const trade of tradeSource) yield trade;
    return;
  }
  if (typeof tradeSource[Symbol.iterator] === 'function') {
    for (const trade of tradeSource) yield trade;
  }
}

import { HistoricalCoverageError } from './historicalBacktestDataService.js';
import { HistoricalMarketReplay } from './historicalMarketReplay.js';
import { timeframeToSeconds } from '../sessionAnalytics.js';

const PROGRESS_EMIT_EVERY_CANDLES = 25;
const YIELD_EVERY_CANDLES = 100;

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
    const replayDays = coveragePlan?.days?.length ? coveragePlan.days : [...iterateUtcDays(startDate, endDate)].map((day) => ({ ...day, targetEndMs: day.dayEndMs }));
    const runState = this.executionEngine.createRunState({ strategy, runConfig });
    const totalDays = replayDays.length;
    const timeframeSec = timeframeToSeconds(strategy.market?.timeframe || '1m');
    const candlesPerDay = Math.floor(86400 / timeframeSec);
    const totalCandlesTarget = Math.max(totalDays * candlesPerDay, 1);
    const sessionResults = [];
    const startedAtMs = Date.parse(runConfig.startedAtIso || new Date().toISOString());

    let processedCandles = 0;
    let emittedSinceProgress = 0;
    let emittedSinceYield = 0;

    const emitProgress = ({
      currentDate,
      currentDay,
      stage,
      replay = null,
      hydration = null,
      progressPct = null
    }) => {
      progressCallback?.({
        processed: processedCandles,
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
        phase: 'replaying',
        hydration,
        replay
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
          processedCandles,
          totalCandlesTarget,
          percent: totalCandlesTarget ? (processedCandles / totalCandlesTarget) * 100 : 0
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
              processedCandles,
              totalCandlesTarget,
              percent: totalCandlesTarget ? (processedCandles / totalCandlesTarget) * 100 : 0
            },
            progressPct: payload.progressPct
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

      for await (const trade of iterateTrades(tradeSource)) {
        shouldStop?.();
        replayedTradeCount += 1;
        if (sessionReplay.seedPrice == null && Number.isFinite(Number(trade?.price))) {
          sessionReplay.seedPrice = Number(trade.price);
        }
        const closedCandles = sessionReplay.processTrade(trade);
        ({ processedCandles, emittedSinceProgress, emittedSinceYield } = await this.#drainCandles({
          closedCandles,
          strategy,
          runState,
          processedCandles,
          emittedSinceProgress,
          emittedSinceYield,
          emitProgress,
          currentDate: isoDate,
          currentDay: dayIndex + 1,
          progressStage: `Replaying session ${Math.min(replayedTradeCount, totalDayTrades)}/${totalDayTrades} trades`,
          totalDayTrades,
          replayedTradeCount,
          totalCandlesTarget
        }));
      }

      ({ processedCandles, emittedSinceProgress, emittedSinceYield } = await this.#drainCandles({
        closedCandles: sessionReplay.flushRemaining(),
        strategy,
        runState,
        processedCandles,
        emittedSinceProgress,
        emittedSinceYield,
        emitProgress,
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        progressStage: 'Flushing end-of-day candles',
        totalDayTrades,
        replayedTradeCount,
        totalCandlesTarget
      }));

      const flattenedTrade = this.executionEngine.finalizeDay({
        strategy,
        state: runState,
        fillModel: this.executionEngine.createFillModel({
          syntheticSpreadBps: runState.settings.syntheticSpreadBps
        }),
        dateLabel: isoDate
      });

      const dayTradesClosed = runState.trades.slice(dayTradeStartIndex);
      sessionResults.push(buildSessionResult({
        isoDate,
        candleCount: candlesPerDay,
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
          processedCandles,
          totalCandlesTarget,
          percent: totalCandlesTarget ? (processedCandles / totalCandlesTarget) * 100 : 100
        }
      });

      await yieldToEventLoop();
    }

    progressCallback?.({
      processed: processedCandles,
      total: totalCandlesTarget,
      progressPct: 95,
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
    processedCandles,
    emittedSinceProgress,
    emittedSinceYield,
    emitProgress,
    currentDate,
    currentDay,
    progressStage,
    totalDayTrades,
    replayedTradeCount,
    totalCandlesTarget
  }) {
    const fillModel = this.executionEngine.createFillModel({
      syntheticSpreadBps: runState.settings.syntheticSpreadBps
    });

    for (const candle of closedCandles) {
      this.executionEngine.processCandle({
        strategy,
        state: runState,
        candle,
        fillModel,
        currentDateLabel: currentDate
      });

      processedCandles += 1;
      emittedSinceProgress += 1;
      emittedSinceYield += 1;

      if (emittedSinceProgress >= PROGRESS_EMIT_EVERY_CANDLES) {
        emitProgress({
          currentDate,
          currentDay,
          stage: progressStage,
          replay: {
            day: currentDate,
            status: 'running',
            replayedTrades: replayedTradeCount,
            totalTrades: totalDayTrades,
            processedCandles,
            totalCandlesTarget,
            percent: totalCandlesTarget ? (processedCandles / totalCandlesTarget) * 100 : 0
          }
        });
        emittedSinceProgress = 0;
      }

      if (emittedSinceYield >= YIELD_EVERY_CANDLES) {
        await yieldToEventLoop();
        emittedSinceYield = 0;
      }
    }

    return { processedCandles, emittedSinceProgress, emittedSinceYield };
  }
}

function computeReplayProgressPct({ currentDay, totalDays, replayPercent }) {
  const safeCurrentDay = Math.max(Number(currentDay) || 1, 1);
  const safeTotalDays = Math.max(Number(totalDays) || 1, 1);
  const completedDays = Math.min(safeCurrentDay - 1, safeTotalDays);
  const replayRatio = clampRatio((Number(replayPercent) || 0) / 100);
  const dayRatio = 0.5 + (replayRatio * 0.45);
  return ((completedDays + clampRatio(dayRatio)) / safeTotalDays) * 100;
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

function yieldToEventLoop() {
  return new Promise((resolve) => setTimeout(resolve, 0));
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

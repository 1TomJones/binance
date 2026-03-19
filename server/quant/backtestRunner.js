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

  async run({ strategy, runConfig, progressCallback, shouldStop }) {
    const startDate = normalizeDay(runConfig.startDate);
    const endDate = normalizeDay(runConfig.endDate || runConfig.startDate);
    const runState = this.executionEngine.createRunState({ strategy, runConfig });
    const totalDays = daysBetweenInclusive(startDate, endDate);
    const timeframeSec = timeframeToSeconds(strategy.market?.timeframe || '1m');
    const candlesPerDay = Math.floor(86400 / timeframeSec);
    const totalCandlesTarget = Math.max(totalDays * candlesPerDay, 1);
    const sessionResults = [];
    const startedAtMs = Date.parse(runConfig.startedAtIso || new Date().toISOString());

    let processedCandles = 0;
    let emittedSinceProgress = 0;
    let emittedSinceYield = 0;
    let lastHydrationState = null;

    const emitProgress = ({
      currentDate,
      currentDay,
      stage,
      phase = 'replay',
      hydration = lastHydrationState,
      replay = null,
      progressPct = null
    }) => {
      progressCallback?.({
        processed: processedCandles,
        total: totalCandlesTarget,
        progressPct: Number.isFinite(Number(progressPct))
          ? Number(progressPct)
          : computeOverallProgressPct({
              currentDay,
              totalDays,
              phase,
              hydrationPercent: hydration?.percent,
              replayPercent: replay?.percent
            }),
        currentDate,
        currentDay,
        totalDays,
        totalTrades: runState.trades.length,
        elapsedMs: Date.now() - startedAtMs,
        marker: stage,
        phase,
        hydration,
        replay
      });
    };

    for (const { dayStartMs, dayEndMs, isoDate, dayIndex } of iterateUtcDays(startDate, endDate)) {
      shouldStop?.();
      lastHydrationState = {
        day: isoDate,
        status: 'pending',
        source: null,
        rowsIngested: 0,
        pagesIngested: 0,
        checkpointTimeMs: null,
        lastAggTradeId: null,
        retry: null,
        percent: 0
      };
      emitProgress({
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        stage: 'Loading session history',
        phase: 'hydration',
        hydration: lastHydrationState,
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

      const dayPayload = this.historicalDataService
        ? await this.historicalDataService.loadDay({
            symbol: strategy.market.symbol,
            dayStartMs,
            dayEndMs,
            shouldStop,
            progressCallback: (payload = {}) => {
              lastHydrationState = {
                ...lastHydrationState,
                ...(payload.hydration || {}),
                day: isoDate
              };
              emitProgress({
                currentDate: isoDate,
                currentDay: dayIndex + 1,
                stage: payload.stage,
                phase: payload.phase || 'hydration',
                hydration: lastHydrationState,
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
            }
          })
        : {
            trades: this.loadTrades({
              symbol: strategy.market.symbol,
              startMs: dayStartMs,
              endMs: dayEndMs,
              limit: null
            }),
            seedTrade: this.loadSeedTrade?.({
              symbol: strategy.market.symbol,
              beforeMs: dayStartMs
            }) || null
          };

      const tradeSource = dayPayload.tradeStream || dayPayload.trades || [];
      const seedTrade = dayPayload.seedTrade || null;
      const totalDayTrades = Math.max(Number(dayPayload.tradeCount ?? (Array.isArray(tradeSource) ? tradeSource.length : 0)) || 0, 1);
      lastHydrationState = {
        ...lastHydrationState,
        status: 'complete',
        percent: 100,
        source: dayPayload.hydrationSource || lastHydrationState?.source || null,
        checkpointTimeMs: dayPayload.targetEndMs || lastHydrationState?.checkpointTimeMs || null,
        lastAggTradeId: dayPayload.lastAggTradeId || lastHydrationState?.lastAggTradeId || null,
        totalTrades: totalDayTrades
      };

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
        phase: 'replay',
        hydration: lastHydrationState,
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
          phase: 'replay',
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

function computeOverallProgressPct({ currentDay, totalDays, phase, hydrationPercent, replayPercent }) {
  const safeCurrentDay = Math.max(Number(currentDay) || 1, 1);
  const safeTotalDays = Math.max(Number(totalDays) || 1, 1);
  const completedDays = Math.min(safeCurrentDay - 1, safeTotalDays);
  const hydrationRatio = clampRatio((Number(hydrationPercent) || 0) / 100);
  const replayRatio = clampRatio((Number(replayPercent) || 0) / 100);

  let dayRatio = replayRatio;
  if (phase === 'hydration') {
    dayRatio = hydrationRatio * 0.4;
  } else if (phase === 'replay') {
    dayRatio = 0.4 + (replayRatio * 0.6);
  } else if (phase === 'completed') {
    dayRatio = 1;
  }

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

function daysBetweenInclusive(startDate, endDate) {
  return Math.floor((endDate.getTime() - startDate.getTime()) / 86400000) + 1;
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

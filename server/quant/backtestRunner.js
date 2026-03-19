import { HistoricalMarketReplay } from './historicalMarketReplay.js';
import { timeframeToSeconds } from '../sessionAnalytics.js';

const PROGRESS_EMIT_EVERY_CANDLES = 25;
const YIELD_EVERY_CANDLES = 100;

export class BacktestRunner {
  constructor({ executionEngine, loadTrades, loadSeedTrade }) {
    this.executionEngine = executionEngine;
    this.loadTrades = loadTrades;
    this.loadSeedTrade = loadSeedTrade;
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

    const emitProgress = ({ currentDate, currentDay, stage }) => {
      progressCallback?.({
        processed: processedCandles,
        total: totalCandlesTarget,
        currentDate,
        currentDay,
        totalDays,
        totalTrades: runState.trades.length,
        elapsedMs: Date.now() - startedAtMs,
        marker: stage
      });
    };

    for (const { dayStartMs, dayEndMs, isoDate, dayIndex } of iterateUtcDays(startDate, endDate)) {
      shouldStop?.();
      emitProgress({ currentDate: isoDate, currentDay: dayIndex + 1, stage: 'Loading session history' });

      const dayTrades = this.loadTrades({
        symbol: strategy.market.symbol,
        startMs: dayStartMs,
        endMs: dayEndMs,
        limit: null
      });

      const seedTrade = this.loadSeedTrade?.({
        symbol: strategy.market.symbol,
        beforeMs: dayStartMs
      }) || null;

      const sessionReplay = new HistoricalMarketReplay({
        timeframe: strategy.market.timeframe,
        sessionStartMs: dayStartMs,
        sessionEndMs: dayEndMs,
        seedPrice: seedTrade?.price ?? dayTrades[0]?.price ?? null,
        settings: runState.settings
      });

      const dayTradeStartIndex = runState.trades.length;
      const totalDayTrades = Math.max(dayTrades.length, 1);

      for (let tradeIndex = 0; tradeIndex < dayTrades.length; tradeIndex += 1) {
        shouldStop?.();
        const closedCandles = sessionReplay.processTrade(dayTrades[tradeIndex]);
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
          progressStage: `Replaying session ${tradeIndex + 1}/${totalDayTrades} trades`
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
        progressStage: 'Flushing end-of-day candles'
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
        sourceTradeCount: dayTrades.length,
        flattenedTrade
      }));

      emitProgress({
        currentDate: isoDate,
        currentDay: dayIndex + 1,
        stage: flattenedTrade ? 'Session flattened at UTC close' : 'Session complete'
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
    progressStage
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
        emitProgress({ currentDate, currentDay, stage: progressStage });
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

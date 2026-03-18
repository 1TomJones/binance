import { enrichCandlesFromTrades } from './candleEnrichment.js';

export class BacktestRunner {
  constructor({ executionEngine, loadTrades }) {
    this.executionEngine = executionEngine;
    this.loadTrades = loadTrades;
  }

  run({ strategy, runConfig, progressCallback, shouldStop }) {
    const startDate = normalizeDay(runConfig.startDate);
    const endDate = normalizeDay(runConfig.endDate || runConfig.startDate);
    const runState = this.executionEngine.createRunState({ strategy, runConfig });
    const totalDays = daysBetweenInclusive(startDate, endDate);
    const totalUnits = totalDays * 1000;
    const dayResults = [];

    forEachUtcDay(startDate, endDate, ({ dayStartMs, dayEndMs, isoDate, dayIndex }) => {
      shouldStop?.();
      const dayTrades = this.loadTrades({
        symbol: strategy.market.symbol,
        startMs: dayStartMs,
        endMs: dayEndMs,
        limit: null
      });
      const candles = enrichCandlesFromTrades(dayTrades, strategy.market.timeframe, runState.settings, {
        sessionStartMs: dayStartMs,
        nowMs: dayEndMs
      });

      const fillModel = this.executionEngine.createFillModel({
        syntheticSpreadBps: runState.settings.syntheticSpreadBps
      });

      candles.forEach((candle, candleIndex) => {
        shouldStop?.();
        this.executionEngine.processCandle({
          strategy,
          state: runState,
          candle,
          fillModel,
          currentDateLabel: isoDate
        });
        const dayProgress = candles.length ? Math.floor(((candleIndex + 1) / candles.length) * 1000) : 1000;
        progressCallback?.({
          processed: dayIndex * 1000 + dayProgress,
          total: totalUnits,
          currentDate: isoDate,
          totalTrades: runState.trades.length,
          elapsedMs: Date.now() - Date.parse(runConfig.startedAtIso || new Date().toISOString()),
          marker: `Simulating ${isoDate} · candle ${candleIndex + 1}/${candles.length}`,
          dayIndex: dayIndex + 1,
          totalDays
        });
      });

      const endOfDayClose = this.executionEngine.finalizeDay({
        strategy,
        state: runState,
        fillModel,
        dateLabel: isoDate
      });

      dayResults.push({
        date: isoDate,
        tradeCount: runState.trades.filter((trade) => trade.entryDate === isoDate).length,
        candleCount: candles.length,
        endOfDayExit: endOfDayClose ? endOfDayClose.exitReason : null
      });
    });

    const result = this.executionEngine.finalizeRun({
      strategy,
      state: runState,
      lastPrice: runState.session.previousCandle?.close || null
    });

    return {
      ...result,
      dayResults
    };
  }
}

function normalizeDay(value) {
  const date = new Date(value);
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function forEachUtcDay(startDate, endDate, callback) {
  let cursor = startDate.getTime();
  let dayIndex = 0;
  while (cursor <= endDate.getTime()) {
    const dayStartMs = cursor;
    const dayEndMs = cursor + 86400000 - 1;
    callback({
      dayStartMs,
      dayEndMs,
      isoDate: new Date(dayStartMs).toISOString().slice(0, 10),
      dayIndex
    });
    cursor += 86400000;
    dayIndex += 1;
  }
}

function daysBetweenInclusive(startDate, endDate) {
  return Math.floor((endDate.getTime() - startDate.getTime()) / 86400000) + 1;
}

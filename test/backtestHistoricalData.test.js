import test from 'node:test';
import assert from 'node:assert/strict';

import { BacktestRunner } from '../server/quant/backtestRunner.js';
import { HistoricalBacktestDataService, HistoricalCoverageError } from '../server/quant/historicalBacktestDataService.js';
import { StrategyExecutionEngine } from '../server/quant/strategyExecutionEngine.js';

const DAY_MS = 86_400_000;

function buildStrategy() {
  return {
    market: {
      symbol: 'BTCUSDT',
      timeframe: '1m',
      allow_long: true,
      allow_short: false
    },
    execution: {
      cooldown_bars_after_exit: 0
    },
    risk: {
      stop_loss_pct: 50,
      take_profit_pct: 50,
      max_holding_bars: 9_999
    },
    positionManagement: {
      enable_break_even: false,
      move_stop_to_break_even_at_profit_pct: 0
    },
    entryRules: {
      long: {
        all: [
          { left: 'close', operator: 'gt', right: 'prev_close' }
        ]
      },
      short: {
        any: []
      }
    },
    exitRules: {
      long: {
        any: []
      },
      short: {
        any: []
      }
    },
    backtestDefaults: {
      initial_balance: 10_000
    }
  };
}

function buildDayTrades(dayStartMs, prices) {
  return prices.map((price, index) => ({
    trade_id: dayStartMs + index + 1,
    symbol: 'BTCUSDT',
    price,
    quantity: 1,
    trade_time: dayStartMs + (index * 60_000) + 10_000,
    maker_flag: 0,
    side: 'buy',
    ingest_ts: dayStartMs + index
  }));
}

function createRunnerWithDayMap(dayMap) {
  return new BacktestRunner({
    executionEngine: new StrategyExecutionEngine(),
    historicalDataService: {
      async loadDay({ dayStartMs }) {
        const payload = dayMap.get(dayStartMs);
        if (!payload) throw new Error(`Missing test payload for ${dayStartMs}`);
        return payload;
      }
    },
    loadTrades: () => [],
    loadSeedTrade: () => null
  });
}

test('backtest replays a fully historical day before the current UTC day and still produces trades', async () => {
  const strategy = buildStrategy();
  const dayStartMs = Date.UTC(2026, 2, 15);
  const runner = createRunnerWithDayMap(new Map([
    [dayStartMs, {
      trades: buildDayTrades(dayStartMs, [100, 101]),
      seedTrade: {
        trade_id: dayStartMs - 1,
        symbol: 'BTCUSDT',
        price: 99,
        quantity: 0.25,
        trade_time: dayStartMs - 1,
        maker_flag: 0,
        side: 'buy',
        ingest_ts: dayStartMs - 1
      }
    }]
  ]));

  const result = await runner.run({
    strategy,
    runConfig: {
      startDate: '2026-03-15',
      endDate: '2026-03-15',
      enableShort: false,
      startedAtIso: '2026-03-19T00:00:00.000Z'
    }
  });

  assert.equal(result.sessionResults.length, 1);
  assert.equal(result.sessionResults[0].date, '2026-03-15');
  assert.equal(result.sessionResults[0].sourceTradeCount, 2);
  assert.ok(result.trades.length > 0, 'expected at least one market-order paper trade from historical-only replay');
  assert.equal(result.trades[0].exitReason, 'end_of_day_exit');
});

test('backtest preserves chronological multi-day replay across several UTC sessions', async () => {
  const strategy = buildStrategy();
  const firstDayMs = Date.UTC(2026, 2, 15);
  const secondDayMs = firstDayMs + DAY_MS;
  const runner = createRunnerWithDayMap(new Map([
    [firstDayMs, {
      trades: buildDayTrades(firstDayMs, [100, 101]),
      seedTrade: {
        trade_id: firstDayMs - 1,
        symbol: 'BTCUSDT',
        price: 99,
        quantity: 0.25,
        trade_time: firstDayMs - 1,
        maker_flag: 0,
        side: 'buy',
        ingest_ts: firstDayMs - 1
      }
    }],
    [secondDayMs, {
      trades: buildDayTrades(secondDayMs, [102, 103]),
      seedTrade: {
        trade_id: secondDayMs - 1,
        symbol: 'BTCUSDT',
        price: 101,
        quantity: 0.25,
        trade_time: secondDayMs - 1,
        maker_flag: 0,
        side: 'buy',
        ingest_ts: secondDayMs - 1
      }
    }]
  ]));

  const result = await runner.run({
    strategy,
    runConfig: {
      startDate: '2026-03-15',
      endDate: '2026-03-16',
      enableShort: false,
      startedAtIso: '2026-03-19T00:00:00.000Z'
    }
  });

  assert.deepEqual(result.sessionResults.map((entry) => entry.date), ['2026-03-15', '2026-03-16']);
  assert.equal(result.sessionResults.length, 2);
  assert.ok(result.sessionResults.every((entry) => entry.sourceTradeCount === 2));
  assert.ok(result.trades.length >= 2, 'expected closed trades across both historical sessions');
  assert.ok(result.trades[0].entryTime < result.trades.at(-1).entryTime, 'trades should remain chronological across replayed days');
});

test('historical coverage failures raise a clear explicit error instead of replaying an empty session', async () => {
  const storedTrades = [];
  const service = new HistoricalBacktestDataService({
    getHistoricalCoverage: () => null,
    saveHistoricalCoverage: () => null,
    loadTradesByRange: (_symbol, startMs, endMs) => storedTrades.filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs),
    loadLatestTradeBefore: () => null,
    getTradeStatsByRange: (_symbol, startMs, endMs) => {
      const trades = storedTrades.filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs);
      return {
        count: trades.length,
        minTradeTime: trades[0]?.trade_time ?? null,
        maxTradeTime: trades.at(-1)?.trade_time ?? null
      };
    },
    saveTradesBatch: (trades) => storedTrades.push(...trades),
    fetchImpl: async () => ({
      ok: true,
      async json() {
        return [];
      }
    }),
    restBaseUrls: ['https://example.test/api/v3'],
    now: () => Date.UTC(2026, 2, 19)
  });

  await assert.rejects(
    () => service.loadDay({
      symbol: 'BTCUSDT',
      dayStartMs: Date.UTC(2026, 2, 10),
      dayEndMs: Date.UTC(2026, 2, 10, 23, 59, 59, 999)
    }),
    (error) => {
      assert.ok(error instanceof HistoricalCoverageError);
      assert.match(error.message, /Unable to hydrate historical trades/);
      return true;
    }
  );
});

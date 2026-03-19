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

function createInMemoryHistoricalService({
  fetchImpl,
  now = () => Date.UTC(2026, 2, 19),
  loadLatestTradeBefore = () => ({
    trade_id: 0,
    symbol: 'BTCUSDT',
    price: 99,
    quantity: 0.25,
    trade_time: Date.UTC(2026, 2, 9, 23, 59, 59, 999),
    maker_flag: 0,
    side: 'buy',
    ingest_ts: 1
  }),
  retryBaseDelayMs = 10,
  sleep = async () => {},
  streamTradesByRange = null,
  loadTradesByRange = null,
  streamChunkSize = 128
} = {}) {
  const storedTrades = [];
  const coverageRecords = [];

  const loadRange = loadTradesByRange || ((_symbol, startMs, endMs) => (
    storedTrades
      .filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs)
      .sort((a, b) => (a.trade_time - b.trade_time) || (a.trade_id - b.trade_id))
  ));

  const service = new HistoricalBacktestDataService({
    getHistoricalCoverage: () => null,
    saveHistoricalCoverage: (record) => {
      coverageRecords.push(record);
      return record;
    },
    loadTradesByRange: loadRange,
    loadLatestTradeBefore,
    getTradeStatsByRange: (_symbol, startMs, endMs) => {
      const trades = storedTrades
        .filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs)
        .sort((a, b) => (a.trade_time - b.trade_time) || (a.trade_id - b.trade_id));
      return {
        count: trades.length,
        minTradeTime: trades[0]?.trade_time ?? null,
        maxTradeTime: trades.at(-1)?.trade_time ?? null
      };
    },
    saveTradesBatch: (trades) => storedTrades.push(...trades),
    streamTradesByRange,
    fetchImpl,
    restBaseUrls: ['https://example.test/api/v3'],
    now,
    retryBaseDelayMs,
    sleep,
    streamChunkSize
  });

  return { service, storedTrades, coverageRecords };
}

function buildAggTrade(id, tradeTime, price = 100 + (id / 10_000), maker = false) {
  return {
    a: id,
    p: price.toFixed(2),
    q: '0.50000000',
    T: tradeTime,
    m: maker
  };
}

function createJsonResponse(status, body, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get(name) {
        return headers[String(name).toLowerCase()] ?? null;
      }
    },
    async json() {
      return body;
    },
    async text() {
      return typeof body === 'string' ? body : JSON.stringify(body);
    }
  };
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
  const { service } = createInMemoryHistoricalService({
    fetchImpl: async () => createJsonResponse(200, [])
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

test('historical hydration retries recoverable Binance REST failures with backoff', async () => {
  const dayStartMs = Date.UTC(2026, 2, 10);
  const sleepCalls = [];
  let attempts = 0;

  const { service, storedTrades } = createInMemoryHistoricalService({
    retryBaseDelayMs: 25,
    sleep: async (ms) => { sleepCalls.push(ms); },
    fetchImpl: async () => {
      attempts += 1;
      if (attempts === 1) return createJsonResponse(500, { msg: 'server busy' });
      if (attempts === 2) return createJsonResponse(429, { msg: 'too many requests' }, { 'retry-after': '1' });
      return createJsonResponse(200, [buildAggTrade(1, dayStartMs + 1_000)]);
    }
  });

  const payload = await service.loadDay({
    symbol: 'BTCUSDT',
    dayStartMs,
    dayEndMs: dayStartMs + DAY_MS - 1
  });

  assert.equal(attempts, 3);
  assert.deepEqual(sleepCalls, [25, 1000]);
  assert.equal(payload.tradeCount, 1);
  assert.equal(storedTrades.length, 1);
  assert.equal(storedTrades[0].trade_id, 1);
});

test('historical hydration paginates full BTCUSDT UTC days without skipping or overrunning the range', async () => {
  const dayStartMs = Date.UTC(2026, 2, 10);
  const dayEndMs = dayStartMs + DAY_MS - 1;
  const requests = [];
  const fullDayTrades = Array.from({ length: 2500 }, (_, index) => {
    const tradeId = index + 1;
    const tradeTime = dayStartMs + Math.floor(index * ((DAY_MS - 10_000) / 2500));
    return buildAggTrade(tradeId, tradeTime);
  });
  const overrunTrades = Array.from({ length: 20 }, (_, index) => buildAggTrade(2501 + index, dayEndMs + 1 + index));

  const { service, storedTrades, coverageRecords } = createInMemoryHistoricalService({
    fetchImpl: async (url) => {
      const parsed = new URL(url);
      requests.push(parsed.search);
      const fromId = parsed.searchParams.get('fromId');
      if (!fromId) {
        return createJsonResponse(200, fullDayTrades.slice(0, 1000));
      }
      if (fromId === '1001') {
        return createJsonResponse(200, fullDayTrades.slice(1000, 2000));
      }
      if (fromId === '2001') {
        return createJsonResponse(200, fullDayTrades.slice(2000).concat(overrunTrades));
      }
      return createJsonResponse(200, []);
    }
  });

  const payload = await service.loadDay({ symbol: 'BTCUSDT', dayStartMs, dayEndMs });
  const replayedTrades = Array.isArray(payload.trades) ? payload.trades : await collectAsync(payload.trades);

  assert.equal(storedTrades.length, 2500);
  assert.equal(replayedTrades.length, 2500);
  assert.equal(replayedTrades[0].trade_id, 1);
  assert.equal(replayedTrades.at(-1).trade_id, 2500);
  assert.ok(storedTrades.every((trade) => trade.trade_time >= dayStartMs && trade.trade_time <= dayEndMs));
  assert.deepEqual(
    requests.map((search) => ({
      startTime: new URLSearchParams(search).get('startTime'),
      endTime: new URLSearchParams(search).get('endTime'),
      fromId: new URLSearchParams(search).get('fromId')
    })),
    [
      { startTime: String(dayStartMs), endTime: String(dayEndMs), fromId: null },
      { startTime: null, endTime: null, fromId: '1001' },
      { startTime: null, endTime: null, fromId: '2001' }
    ]
  );
  assert.equal(coverageRecords.at(-1)?.trade_count, 2500);
});

test('backtest replay consumes chunked historical streams incrementally without lookahead', async () => {
  const strategy = buildStrategy();
  const dayStartMs = Date.UTC(2026, 2, 15);
  const engine = new StrategyExecutionEngine();
  const originalProcessCandle = engine.processCandle.bind(engine);
  let processedCandles = 0;

  engine.processCandle = (payload) => {
    processedCandles += 1;
    return originalProcessCandle(payload);
  };

  const trades = buildDayTrades(dayStartMs, [100, 101, 102, 103]);
  const runner = new BacktestRunner({
    executionEngine: engine,
    historicalDataService: {
      async loadDay() {
        return {
          tradeCount: trades.length,
          seedTrade: {
            trade_id: dayStartMs - 1,
            symbol: 'BTCUSDT',
            price: 99,
            quantity: 0.25,
            trade_time: dayStartMs - 1,
            maker_flag: 0,
            side: 'buy',
            ingest_ts: dayStartMs - 1
          },
          trades: (async function* () {
            yield trades[0];
            yield trades[1];
            assert.ok(processedCandles > 0, 'expected earlier candles to be processed before later trades are requested');
            yield trades[2];
            yield trades[3];
          }())
        };
      }
    },
    loadTrades: () => [],
    loadSeedTrade: () => null
  });

  const result = await runner.run({
    strategy,
    runConfig: {
      startDate: '2026-03-15',
      endDate: '2026-03-15',
      enableShort: false,
      startedAtIso: '2026-03-19T00:00:00.000Z'
    }
  });

  assert.equal(result.sessionResults[0].sourceTradeCount, 4);
  assert.ok(processedCandles > 0);
});

test('multi-day backtests stream historical SQLite trades in bounded chunks instead of loading full days into memory', async () => {
  const strategy = buildStrategy();
  const firstDayMs = Date.UTC(2026, 2, 15);
  const dayMap = new Map([
    [firstDayMs, buildDayTrades(firstDayMs, Array.from({ length: 256 }, (_, index) => 100 + index / 100))],
    [firstDayMs + DAY_MS, buildDayTrades(firstDayMs + DAY_MS, Array.from({ length: 256 }, (_, index) => 103 + index / 100))],
    [firstDayMs + (2 * DAY_MS), buildDayTrades(firstDayMs + (2 * DAY_MS), Array.from({ length: 256 }, (_, index) => 106 + index / 100))]
  ]);

  const pageLoads = [];
  const { service } = createInMemoryHistoricalService({
    fetchImpl: async () => createJsonResponse(200, []),
    loadLatestTradeBefore: (_symbol, beforeMs) => ({
      trade_id: beforeMs - 1,
      symbol: 'BTCUSDT',
      price: 99,
      quantity: 0.25,
      trade_time: beforeMs - 1,
      maker_flag: 0,
      side: 'buy',
      ingest_ts: beforeMs - 1
    }),
    loadTradesByRange: () => {
      throw new Error('loadTradesByRange should not be used when chunked streaming is enabled');
    },
    streamTradesByRange: (symbol, startMs, endMs, chunkSize) => (async function* () {
      const rows = dayMap.get(startMs).filter((trade) => trade.symbol === symbol && trade.trade_time >= startMs && trade.trade_time <= endMs);
      for (let index = 0; index < rows.length; index += chunkSize) {
        const chunk = rows.slice(index, index + chunkSize);
        pageLoads.push(chunk.length);
        for (const trade of chunk) yield trade;
      }
    }()),
    streamChunkSize: 64
  });

  for (const trades of dayMap.values()) {
    service.saveTradesBatch(trades);
  }

  const runner = new BacktestRunner({
    executionEngine: new StrategyExecutionEngine(),
    historicalDataService: service,
    loadTrades: () => [],
    loadSeedTrade: () => null
  });

  const result = await runner.run({
    strategy,
    runConfig: {
      startDate: '2026-03-15',
      endDate: '2026-03-17',
      enableShort: false,
      startedAtIso: '2026-03-19T00:00:00.000Z'
    }
  });

  assert.equal(result.sessionResults.length, 3);
  assert.ok(pageLoads.length >= 12, 'expected multiple chunk loads across days');
  assert.ok(Math.max(...pageLoads) <= 64, 'expected chunk loader to stay within the configured page size');
});

async function collectAsync(iterable) {
  const rows = [];
  for await (const value of iterable) rows.push(value);
  return rows;
}

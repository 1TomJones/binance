import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { deflateRawSync } from 'node:zlib';

import { BacktestRunner } from '../server/quant/backtestRunner.js';
import { HistoricalBacktestDataService, HistoricalCoverageError } from '../server/quant/historicalBacktestDataService.js';
import { BacktestJobService } from '../server/quant/backtestJobService.js';
import { LivePaperRunner } from '../server/quant/livePaperRunner.js';
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
  streamChunkSize = 128,
  bulkDataBaseUrl = 'https://bulk.example.test',
  initialCoverage = null
} = {}) {
  const storedTrades = [];
  const coverageRecords = [];
  const coverageStore = new Map();
  if (initialCoverage instanceof Map) {
    for (const [dayStartMs, record] of initialCoverage.entries()) coverageStore.set(dayStartMs, { ...record });
  } else if (Array.isArray(initialCoverage)) {
    for (const record of initialCoverage) coverageStore.set(record.day_start_ms, { ...record });
  } else if (initialCoverage?.day_start_ms != null) {
    coverageStore.set(initialCoverage.day_start_ms, { ...initialCoverage });
  }

  const loadRange = loadTradesByRange || ((_symbol, startMs, endMs) => (
    storedTrades
      .filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs)
      .sort((a, b) => (a.trade_time - b.trade_time) || (a.trade_id - b.trade_id))
  ));

  const service = new HistoricalBacktestDataService({
    getHistoricalCoverage: (_symbol, dayStartMs) => coverageStore.get(dayStartMs) || null,
    saveHistoricalCoverage: (record) => {
      coverageRecords.push(record);
      coverageStore.set(record.day_start_ms, { ...record });
      return coverageStore.get(record.day_start_ms);
    },
    loadTradesByRange: loadRange,
    loadLatestTradeBefore,
    loadLatestTradeInRange: (_symbol, startMs, endMs) => (
      storedTrades
        .filter((trade) => trade.trade_time >= startMs && trade.trade_time <= endMs)
        .sort((a, b) => (a.trade_time - b.trade_time) || (a.trade_id - b.trade_id))
        .at(-1) || null
    ),
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
    bulkDataBaseUrl,
    now,
    retryBaseDelayMs,
    sleep,
    streamChunkSize
  });

  return {
    service,
    storedTrades,
    coverageRecords,
    getCoverageRecord: (dayStartMs = null) => (dayStartMs == null ? [...coverageStore.values()].at(-1) || null : coverageStore.get(dayStartMs) || null)
  };
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

function createStreamResponse(status, buffer, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get(name) {
        const key = String(name).toLowerCase();
        if (key === 'content-length') return String(buffer.length);
        return headers[key] ?? null;
      }
    },
    body: Readable.toWeb(Readable.from([buffer])),
    async text() {
      return buffer.toString('utf8');
    },
    async json() {
      return JSON.parse(buffer.toString('utf8'));
    }
  };
}

function createBulkZipResponse(lines) {
  const csvBody = `${lines.join('\n')}\n`;
  const csvBuffer = Buffer.from(csvBody, 'utf8');
  const compressed = deflateRawSync(csvBuffer);
  const fileName = Buffer.from('BTCUSDT-aggTrades.csv', 'utf8');
  const header = Buffer.alloc(30);
  header.writeUInt32LE(0x04034b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(0, 6);
  header.writeUInt16LE(8, 8);
  header.writeUInt32LE(0, 10);
  header.writeUInt32LE(0, 14);
  header.writeUInt32LE(compressed.length, 18);
  header.writeUInt32LE(csvBuffer.length, 22);
  header.writeUInt16LE(fileName.length, 26);
  header.writeUInt16LE(0, 28);
  return createStreamResponse(200, Buffer.concat([header, fileName, compressed]));
}

function buildBulkAggTradeCsvLine(id, tradeTime, price = 100 + (id / 10_000), maker = false) {
  return [id, price.toFixed(2), '0.50000000', id, id, tradeTime, maker ? 'true' : 'false', 'true'].join(',');
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
    bulkDataBaseUrl: null,
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
    bulkDataBaseUrl: null,
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

test('completed UTC day hydration prefers bulk-file source over REST', async () => {
  const dayStartMs = Date.UTC(2026, 2, 10);
  const dayEndMs = dayStartMs + DAY_MS - 1;
  const calls = [];
  const bulkLines = [
    'agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker,was_best_price_match',
    buildBulkAggTradeCsvLine(1, dayStartMs + 1_000),
    buildBulkAggTradeCsvLine(2, dayStartMs + 61_000, 101, true),
    buildBulkAggTradeCsvLine(3, dayEndMs, 102, false)
  ];

  const { service, storedTrades, getCoverageRecord } = createInMemoryHistoricalService({
    fetchImpl: async (url) => {
      calls.push(url);
      if (String(url).endsWith('.zip')) return createBulkZipResponse(bulkLines);
      throw new Error('REST should not be used when bulk hydration succeeds.');
    }
  });

  const payload = await service.loadDay({ symbol: 'BTCUSDT', dayStartMs, dayEndMs });
  const replayedTrades = Array.isArray(payload.trades) ? payload.trades : await collectAsync(payload.trades);

  assert.equal(replayedTrades.length, 3);
  assert.equal(storedTrades.length, 3, 'expected bulk hydration to persist the historical day without any REST pagination');
  assert.equal(calls.length, 1);
  assert.match(calls[0], /\.zip$/);
  assert.equal(payload.hydrationSource, 'bulk-file');
  assert.equal(getCoverageRecord()?.source, 'bulk-file');
});

test('partial hydration checkpoint resumes without re-fetching the already persisted prefix', async () => {
  const dayStartMs = Date.UTC(2026, 2, 10);
  const dayEndMs = dayStartMs + DAY_MS - 1;
  const calls = [];
  const { service, storedTrades, getCoverageRecord } = createInMemoryHistoricalService({
    bulkDataBaseUrl: null,
    initialCoverage: {
      symbol: 'BTCUSDT',
      day_start_ms: dayStartMs,
      day_end_ms: dayEndMs,
      coverage_start_ms: dayStartMs,
      coverage_end_ms: dayStartMs + 61_000,
      trade_count: 2,
      first_trade_time: dayStartMs + 1_000,
      last_trade_time: dayStartMs + 61_000,
      last_agg_trade_id: 2,
      checkpoint_time_ms: dayStartMs + 61_000,
      checkpoint_rows: 2,
      status: 'partial',
      source: 'binance-rest'
    },
    fetchImpl: async (url) => {
      calls.push(url);
      const parsed = new URL(url);
      assert.equal(parsed.searchParams.get('fromId'), '3');
      return createJsonResponse(200, [
        buildAggTrade(3, dayStartMs + 121_000),
        buildAggTrade(4, dayStartMs + 181_000)
      ]);
    }
  });

  storedTrades.push(
    {
      trade_id: 1,
      symbol: 'BTCUSDT',
      price: 100,
      quantity: 1,
      trade_time: dayStartMs + 1_000,
      maker_flag: 0,
      side: 'buy',
      ingest_ts: 1
    },
    {
      trade_id: 2,
      symbol: 'BTCUSDT',
      price: 101,
      quantity: 1,
      trade_time: dayStartMs + 61_000,
      maker_flag: 0,
      side: 'buy',
      ingest_ts: 2
    }
  );

  const payload = await service.loadDay({ symbol: 'BTCUSDT', dayStartMs, dayEndMs });
  const replayedTrades = Array.isArray(payload.trades) ? payload.trades : await collectAsync(payload.trades);

  assert.equal(calls.length, 1);
  assert.deepEqual(replayedTrades.map((trade) => trade.trade_id), [1, 2, 3, 4]);
  assert.equal(getCoverageRecord()?.last_agg_trade_id, 4);
  assert.equal(getCoverageRecord()?.checkpoint_time_ms, dayStartMs + 181_000);
});

test('retry/backoff progress messaging is visible while the same page is being retried', async () => {
  const dayStartMs = Date.UTC(2026, 2, 10);
  const events = [];
  let attempts = 0;
  const { service } = createInMemoryHistoricalService({
    bulkDataBaseUrl: null,
    retryBaseDelayMs: 20,
    sleep: async () => {},
    fetchImpl: async () => {
      attempts += 1;
      if (attempts < 3) return createJsonResponse(500, { msg: 'server busy' });
      return createJsonResponse(200, [buildAggTrade(1, dayStartMs + 1_000)]);
    }
  });

  await service.loadDay({
    symbol: 'BTCUSDT',
    dayStartMs,
    dayEndMs: dayStartMs + DAY_MS - 1,
    progressCallback: (payload) => events.push(payload)
  });

  const retryEvent = events.find((payload) => payload.hydration?.status === 'retrying');
  assert.ok(retryEvent, 'expected a retry progress event');
  assert.match(retryEvent.stage, /Retrying Binance REST page 1/);
  assert.equal(retryEvent.hydration.retry.attempt, 1);
  assert.equal(retryEvent.hydration.retry.scope, 'page-1');
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
    bulkDataBaseUrl: null,
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

test('live mode behavior remains unchanged because hydration wiring stays backtest-only', () => {
  const strategy = buildStrategy();
  const runner = new LivePaperRunner({
    getMarketSnapshot: () => ({
      symbol: 'BTCUSDT',
      bestBid: 100,
      bestAsk: 101,
      markPrice: 100.5,
      analysis: {
        candles: [],
        closedCandles: []
      }
    }),
    saveLiveState: () => {},
    getLiveState: () => null,
    strategyResolver: () => ({
      strategy,
      summary: { name: 'Live test strategy' }
    })
  });

  const snapshot = runner.start({
    strategyRef: { kind: 'built_in', key: 'test' },
    runConfig: { orderSize: 0.01, enableLong: true, enableShort: false }
  });

  assert.equal(snapshot.status, 'running');
  assert.equal(snapshot.mode, 'Paper Trading Only');
  assert.equal(snapshot.strategyStatus, 'Monitoring live flow');
});

async function collectAsync(iterable) {
  const rows = [];
  for await (const value of iterable) rows.push(value);
  return rows;
}

test('coverage planner classifies ready, bulk, gap-fill, and current-day tail sessions up front', () => {
  const firstDayMs = Date.UTC(2026, 2, 16);
  const secondDayMs = firstDayMs + DAY_MS;
  const thirdDayMs = secondDayMs + DAY_MS;
  const fourthDayMs = thirdDayMs + DAY_MS;
  const initialCoverage = new Map([
    [firstDayMs, {
      symbol: 'BTCUSDT',
      day_start_ms: firstDayMs,
      day_end_ms: firstDayMs + DAY_MS - 1,
      coverage_start_ms: firstDayMs,
      coverage_end_ms: firstDayMs + DAY_MS - 1,
      trade_count: 10,
      first_trade_time: firstDayMs + 1_000,
      last_trade_time: firstDayMs + DAY_MS - 2_000,
      last_agg_trade_id: 10,
      checkpoint_time_ms: firstDayMs + DAY_MS - 2_000,
      checkpoint_rows: 10,
      status: 'complete',
      source: 'bulk-file'
    }],
    [secondDayMs, {
      symbol: 'BTCUSDT',
      day_start_ms: secondDayMs,
      day_end_ms: secondDayMs + DAY_MS - 1,
      coverage_start_ms: secondDayMs,
      coverage_end_ms: secondDayMs + DAY_MS - 1 - (60 * 1000),
      trade_count: 20,
      first_trade_time: secondDayMs + 1_000,
      last_trade_time: secondDayMs + DAY_MS - 1 - (60 * 1000),
      last_agg_trade_id: 20,
      checkpoint_time_ms: secondDayMs + DAY_MS - 1 - (60 * 1000),
      checkpoint_rows: 20,
      status: 'partial',
      source: 'binance-rest'
    }]
  ]);

  const { service } = createInMemoryHistoricalService({
    now: () => Date.UTC(2026, 2, 19, 12, 0, 0),
    initialCoverage
  });

  const plan = service.planCoverageRange({
    symbol: 'BTCUSDT',
    startDate: '2026-03-16',
    endDate: '2026-03-19',
    includeCurrentDay: true
  });

  assert.equal(plan.days.length, 4);
  assert.equal(plan.days[0].classification, 'already-covered-local');
  assert.equal(plan.days[1].classification, 'small-gap-fill');
  assert.equal(plan.days[2].classification, 'completed-day-bulk');
  assert.equal(plan.days[3].classification, 'current-day-tail');
  assert.equal(plan.readyDays, 1);
  assert.equal(plan.hydratableDays, 3);
  assert.equal(plan.requestedCurrentUtcDay, true);
});

test('current UTC day hydration is explicitly routed through the slower tail path', async () => {
  const dayStartMs = Date.UTC(2026, 2, 19);
  const requests = [];
  const { service, getCoverageRecord } = createInMemoryHistoricalService({
    bulkDataBaseUrl: 'https://bulk.example.test',
    now: () => dayStartMs + (12 * 60 * 60 * 1000),
    fetchImpl: async (url) => {
      requests.push(String(url));
      return createJsonResponse(200, [buildAggTrade(1, dayStartMs + 1_000)]);
    }
  });

  const plan = await service.prepareCoverage({
    symbol: 'BTCUSDT',
    startDate: '2026-03-19',
    endDate: '2026-03-19',
    includeCurrentDay: true
  });
  const payload = await service.loadPreparedDay({
    symbol: 'BTCUSDT',
    dayStartMs,
    dayEndMs: dayStartMs + DAY_MS - 1,
    targetEndMs: plan.days[0].targetEndMs,
    coveragePlanEntry: plan.days[0]
  });

  assert.equal(plan.days[0].classification, 'current-day-tail');
  assert.equal(getCoverageRecord(dayStartMs)?.source, 'current-day-tail');
  assert.equal(payload.hydrationSource, 'current-day-tail');
  assert.ok(requests.every((url) => !url.endsWith('.zip')), 'current-day tail should not use the completed-day bulk ZIP path');
});

test('backtest jobs do not start replay until coverage preparation completes', async () => {
  const progressEvents = [];
  let prepared = false;
  let completedResult = null;
  let failedJob = null;

  const jobState = {
    1: { id: 1, status: 'queued', progress_pct: 0, current_marker: 'Queued' }
  };

  const historicalDataService = {
    async prepareCoverage({ progressCallback }) {
      progressCallback({
        phase: 'planning',
        stage: 'Preparing historical coverage',
        progressPct: 5,
        coverage: {
          totalDays: 2,
          readyDays: 0,
          hydratableDays: 2,
          waitingOnCoverage: true,
          hydratingDay: null,
          includeCurrentDay: false,
          requestedCurrentUtcDay: false,
          currentUtcDaySlowPath: false,
          classifications: []
        },
        currentDate: '2026-03-15',
        currentDay: 1,
        totalDays: 2,
        hydration: null
      });
      prepared = true;
      return {
        totalDays: 2,
        readyDays: 2,
        hydratableDays: 0,
        waitingOnCoverage: false,
        includeCurrentDay: false,
        requestedCurrentUtcDay: false,
        days: [
          { isoDate: '2026-03-15', dayIndex: 0, classification: 'already-covered-local' },
          { isoDate: '2026-03-16', dayIndex: 1, classification: 'already-covered-local' }
        ]
      };
    }
  };

  const service = new BacktestJobService({
    backtestRunner: {
      async run({ progressCallback }) {
        assert.equal(prepared, true, 'replay must not start before coverage preparation finishes');
        progressCallback({
          processed: 0,
          total: 1,
          progressPct: 60,
          marker: 'Replaying historical sessions',
          currentDate: '2026-03-15',
          currentDay: 1,
          totalDays: 2,
          totalTrades: 0,
          elapsedMs: 1,
          phase: 'replaying',
          hydration: null,
          replay: { status: 'running', replayedTrades: 0, totalTrades: 10, percent: 10 }
        });
        return {
          metrics: {},
          analyses: {},
          sessionResults: [],
          cumulativePnlSeries: [],
          equitySeries: [],
          drawdownSeries: [],
          trades: []
        };
      }
    },
    historicalDataService,
    resolveStrategy: () => ({ strategy: buildStrategy(), summary: { key: 'test' } }),
    createJob: () => jobState[1],
    updateJob: (id, patch) => {
      jobState[id] = { ...jobState[id], ...patch };
      progressEvents.push(jobState[id]);
      return jobState[id];
    },
    completeJob: (id, patch) => {
      jobState[id] = { ...jobState[id], ...patch, status: 'completed' };
      completedResult = jobState[id];
      return completedResult;
    },
    failJob: (_id, error) => { failedJob = error; },
    saveResult: () => ({ id: 99 }),
    listJobProgress: () => [],
    getJobById: (id) => jobState[id]
  });

  service.start({
    strategyRef: { kind: 'built_in', key: 'test' },
    runConfig: { startDate: '2026-03-15', endDate: '2026-03-16', includeCurrentDay: false }
  });

  await new Promise((resolve) => setTimeout(resolve, 25));

  assert.equal(failedJob, null);
  assert.equal(prepared, true);
  assert.equal(completedResult?.status, 'completed');
  assert.equal(progressEvents[0]?.current_marker, 'Preparing historical coverage');
  assert.ok(progressEvents.some((event) => event.current_marker === 'Preparing historical coverage'));
  assert.ok(progressEvents.some((event) => event.current_marker === 'Replaying historical sessions'));
});

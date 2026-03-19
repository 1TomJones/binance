import express from 'express';
import cors from 'cors';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { Server } from 'socket.io';
import { BinanceStreamService } from './binanceStream.js';
import {
  getLatestBook,
  getRecentTrades,
  saveBookTicker,
  saveQuantLiveRun,
  saveQuantStrategy,
  saveTrade,
  getQuantStrategyById,
  listQuantStrategies
} from './db.js';
import { StrategyParser } from './quant/strategyParser.js';
import { StrategyExecutionEngine } from './quant/strategyExecutionEngine.js';
import { enrichMarketCandles } from './quant/candleEnrichment.js';
import { LivePaperRunner, LIVE_PAPER_LIMITS } from './quant/livePaperRunner.js';
import { BacktestRunner, BACKTEST_SPEEDS } from './quant/backtestRunner.js';
import { getBuiltInStrategyDefinition, listBuiltInLiveStrategies } from './quant/builtinStrategies.js';
import { StrategyUploadService, StrategyValidationService } from './quant/strategyServices.js';
import {
  buildVolumeProfileFromCandles,
  buildVolumeProfileFromMap,
  computeSessionCvdFromMinuteCandles,
  computeSessionVwapFromCandles,
  aggregateCandles,
  getUtcDayStartMs,
} from './sessionAnalytics.js';
import { deriveSuggestedBacktestRange, normalizeBacktestDateRange } from './marketTime.js';

const PORT = process.env.PORT || 3000;
const SYMBOL = 'BTCUSDT';
const BINANCE_REST_BASES = [
  process.env.BINANCE_REST_URL,
  'https://api.binance.com/api/v3',
  'https://api1.binance.com/api/v3',
  'https://data-api.binance.vision/api/v3'
].filter(Boolean);

class NonRetryableInitializationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'NonRetryableInitializationError';
  }
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json({ limit: '2mb' }));

let latestTrade = null;
let latestBook = null;
let latestDepth = null;
function createSessionState(dayStartMs) {
  return {
    dayStartMs,
    minuteCandles: [],
    minuteCandleIndex: new Map(),
    cvdRunning: 0,
    cvdMinuteCandles: [],
    cvdMinuteIndex: new Map(),
    volumeProfile: new Map(),
    lastProcessedTradeId: null,
    pendingDerivedTrades: [],
    hydration: {
      status: 'idle',
      source: null,
      startedAt: null,
      finishedAt: null,
      fetchedCandleCount: 0,
      fetchedTradeCount: 0,
      mergedCandleCount: 0,
      processedTradeCount: 0,
      lastError: null
    }
  };
}

let sessionState = createSessionState(getUtcDayStartMs());
let sessionInitializationPromise = null;
let sessionInitializationTimer = null;
const SESSION_INITIALIZATION_RETRY_MS = 30_000;
const MARKET_CONTEXT_TTL_MS = 60_000;
const BACKTEST_DEFAULT_LOOKBACK_DAYS = 5;
const marketContext = {
  fetchedAt: 0,
  latestCandleOpenMs: null
};

function recordMarketTimestamp(timestampMs) {
  if (!Number.isFinite(timestampMs)) return;
  if (!Number.isFinite(marketContext.latestCandleOpenMs) || timestampMs > marketContext.latestCandleOpenMs) {
    marketContext.latestCandleOpenMs = timestampMs;
  }
}

function getEffectiveMarketNowMs() {
  if (Number.isFinite(latestTrade?.trade_time)) return latestTrade.trade_time;
  if (Number.isFinite(marketContext.latestCandleOpenMs)) return marketContext.latestCandleOpenMs + 59_000;
  return Date.now();
}

function scheduleSessionInitialization({ reason = 'scheduled', delayMs = 0, targetNowMs } = {}) {
  clearTimeout(sessionInitializationTimer);
  sessionInitializationTimer = setTimeout(() => {
    sessionInitializationTimer = null;
    void initializeCurrentSessionSafe({ reason, targetNowMs });
  }, Math.max(0, delayMs));
}

function ensureCurrentSession(nowMs = getEffectiveMarketNowMs()) {
  const currentDayStart = getUtcDayStartMs(nowMs);
  if (sessionState.dayStartMs !== currentDayStart) {
    sessionState = createSessionState(currentDayStart);
    console.info('[session] rolled to new UTC day', { dayStartIso: new Date(currentDayStart).toISOString() });
    scheduleSessionInitialization({ reason: 'utc-day-rollover', delayMs: 0, targetNowMs: nowMs });
  }
}

function mergeVolumeProfileIntoSession(candles = []) {
  buildVolumeProfileFromCandles(candles).forEach(({ price, volume }) => {
    sessionState.volumeProfile.set(price, (sessionState.volumeProfile.get(price) || 0) + Number(volume || 0));
  });
}


function ensureCvdMinuteCandle(minuteTimeSec) {
  const existingIndex = sessionState.cvdMinuteIndex.get(minuteTimeSec);
  if (existingIndex !== undefined) {
    return sessionState.cvdMinuteCandles[existingIndex];
  }

  const candle = {
    time: minuteTimeSec,
    open: sessionState.cvdRunning,
    high: sessionState.cvdRunning,
    low: sessionState.cvdRunning,
    close: sessionState.cvdRunning,
    hasTrades: false
  };

  sessionState.cvdMinuteIndex.set(minuteTimeSec, sessionState.cvdMinuteCandles.length);
  sessionState.cvdMinuteCandles.push(candle);
  return candle;
}

function mergeCvdMinuteCandlesIntoSession(candles = []) {
  let merged = 0;

  candles.forEach((candle) => {
    if (!candle || !Number.isFinite(candle.time) || candle.time < Math.floor(sessionState.dayStartMs / 1000)) return;

    const existingIndex = sessionState.cvdMinuteIndex.get(candle.time);
    if (existingIndex === undefined) {
      sessionState.cvdMinuteIndex.set(candle.time, sessionState.cvdMinuteCandles.length);
      sessionState.cvdMinuteCandles.push(candle);
      merged += 1;
      return;
    }

    sessionState.cvdMinuteCandles[existingIndex] = candle;
  });

  sessionState.cvdMinuteCandles.sort((a, b) => a.time - b.time);
  sessionState.cvdMinuteIndex = new Map(sessionState.cvdMinuteCandles.map((candle, index) => [candle.time, index]));
  sessionState.cvdRunning = sessionState.cvdMinuteCandles.at(-1)?.close || 0;
  return merged;
}

function getTradeCandleTimeSec(tradeTimeMs, candleIntervalSec = 60) {
  return Math.floor(Math.floor(Number(tradeTimeMs) / 1000) / candleIntervalSec) * candleIntervalSec;
}

function queuePendingDerivedTrade(trade) {
  if (!trade) return;
  sessionState.pendingDerivedTrades.push(trade);
}

function flushPendingDerivedTrades() {
  if (!sessionState.pendingDerivedTrades.length) return 0;

  const queuedTrades = [...sessionState.pendingDerivedTrades]
    .sort((a, b) => a.trade_time - b.trade_time || a.trade_id - b.trade_id);

  sessionState.pendingDerivedTrades = [];

  let processed = 0;
  queuedTrades.forEach((trade) => {
    if (applyTradeToDerivedState(trade)) processed += 1;
  });

  return processed;
}


function applyTradeToMinuteCandle(trade) {
  if (!trade || trade.trade_time < sessionState.dayStartMs) return;
  const minuteTimeSec = getTradeCandleTimeSec(trade.trade_time, 60);
  const existingIndex = sessionState.minuteCandleIndex.get(minuteTimeSec);
  const price = Number(trade.price || 0);
  const quantity = Number(trade.quantity || 0);

  if (existingIndex === undefined) {
    const candle = {
      time: minuteTimeSec,
      open: price,
      high: price,
      low: price,
      close: price,
      volume: quantity,
      hasTrades: true,
      isPlaceholder: false
    };
    sessionState.minuteCandleIndex.set(minuteTimeSec, sessionState.minuteCandles.length);
    sessionState.minuteCandles.push(candle);
    return;
  }

  const candle = sessionState.minuteCandles[existingIndex];
  candle.high = Math.max(Number(candle.high ?? price), price);
  candle.low = Math.min(Number(candle.low ?? price), price);
  candle.close = price;
  candle.volume = Number(candle.volume || 0) + quantity;
  candle.hasTrades = true;
  candle.isPlaceholder = false;
}

function applyTradeToDerivedState(trade) {
  if (!trade || trade.trade_time < sessionState.dayStartMs) return false;
  if (Number.isFinite(sessionState.lastProcessedTradeId) && trade.trade_id <= sessionState.lastProcessedTradeId) return false;

  const quantity = Number(trade.quantity || 0);
  const delta = trade.maker_flag ? -quantity : quantity;
  const minuteTimeSec = getTradeCandleTimeSec(trade.trade_time, 60);

  const cvdCandle = ensureCvdMinuteCandle(minuteTimeSec);
  sessionState.cvdRunning += delta;
  cvdCandle.high = Math.max(cvdCandle.high, sessionState.cvdRunning);
  cvdCandle.low = Math.min(cvdCandle.low, sessionState.cvdRunning);
  cvdCandle.close = sessionState.cvdRunning;
  cvdCandle.hasTrades = true;

  const profileBucket = Math.floor(Number(trade.price));
  sessionState.volumeProfile.set(profileBucket, (sessionState.volumeProfile.get(profileBucket) || 0) + quantity);

  sessionState.lastProcessedTradeId = trade.trade_id;
  sessionState.hydration.processedTradeCount += 1;
  return true;
}

async function fetchBinanceWithFallback(endpointPath, search, { timeoutMs = 10000, context = 'binance-request', baseUrls = BINANCE_REST_BASES } = {}) {
  const params = Object.fromEntries(search.entries());
  let lastFailure = null;

  for (const baseUrl of baseUrls) {
    const normalizedEndpointPath = endpointPath.startsWith('/') ? endpointPath.slice(1) : endpointPath;
    const baseAlreadyTargetsEndpoint = baseUrl.endsWith(`/${normalizedEndpointPath}`);
    const url = baseAlreadyTargetsEndpoint
      ? `${baseUrl}?${search.toString()}`
      : `${baseUrl}${endpointPath}?${search.toString()}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response.ok) {
        const responseBody = await response.text();
        const failure = {
          status: response.status,
          url,
          baseUrl,
          endpointPath,
          params,
          responseBody
        };

        console.error(`[${context}] non-200 response from Binance`, failure);
        lastFailure = failure;

        if (response.status >= 400 && response.status < 500) {
          const message = responseBody || `HTTP ${response.status}`;
          throw new NonRetryableInitializationError(
            `Binance ${endpointPath} request rejected (HTTP ${response.status}): ${message}`
          );
        }

        continue;
      }

      const payload = await response.json();
      return { payload, url, baseUrl };
    } catch (error) {
      if (error instanceof NonRetryableInitializationError) {
        throw error;
      }

      lastFailure = {
        url,
        baseUrl,
        endpointPath,
        params,
        error: error?.message || String(error)
      };
      console.error(`[${context}] request failed`, lastFailure);
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error(
    `Unable to fetch ${endpointPath} from Binance endpoints: ${JSON.stringify(lastFailure)}`
  );
}

async function fetchLatestAvailableCandle({ symbol = SYMBOL, interval = '1m', force = false } = {}) {
  if (!force && Number.isFinite(marketContext.latestCandleOpenMs) && (Date.now() - marketContext.fetchedAt) < MARKET_CONTEXT_TTL_MS) {
    return { time: Math.floor(marketContext.latestCandleOpenMs / 1000) };
  }

  const search = new URLSearchParams({
    symbol,
    interval,
    limit: '1'
  });

  const { payload } = await fetchBinanceWithFallback('/klines', search, {
    context: 'market-context/latest-candle'
  });

  if (!Array.isArray(payload) || !payload.length) {
    throw new Error('Binance returned no candles for the latest candle context request.');
  }

  const latestCandle = normalizeKline(payload[0]);

  marketContext.fetchedAt = Date.now();
  marketContext.latestCandleOpenMs = latestCandle.time * 1000;
  return latestCandle;
}

async function buildSuggestedBacktestConfig() {
  try {
    const latestCandle = await fetchLatestAvailableCandle();
    return deriveSuggestedBacktestRange({
      latestCandleOpenMs: latestCandle.time * 1000,
      lookbackDays: BACKTEST_DEFAULT_LOOKBACK_DAYS
    });
  } catch (_error) {
    const fallbackNowMs = getEffectiveMarketNowMs();
    return deriveSuggestedBacktestRange({
      latestCandleOpenMs: Math.max(sessionState.minuteCandles.at(-1)?.time * 1000 || 0, fallbackNowMs - 60_000),
      lookbackDays: BACKTEST_DEFAULT_LOOKBACK_DAYS
    });
  }
}

async function normalizeBacktestRequestRange({ startDate, endDate }) {
  try {
    const latestCandle = await fetchLatestAvailableCandle();
    return normalizeBacktestDateRange({
      startDate,
      endDate,
      latestCandleOpenMs: latestCandle.time * 1000,
      lookbackDays: BACKTEST_DEFAULT_LOOKBACK_DAYS
    });
  } catch (_error) {
    return normalizeBacktestDateRange({
      startDate,
      endDate,
      latestCandleOpenMs: sessionState.minuteCandles.at(-1)?.time * 1000 || null,
      lookbackDays: BACKTEST_DEFAULT_LOOKBACK_DAYS
    });
  }
}

function buildTimeScaffold(timeframe, sessionStartMs, nowMs) {
  const tfSeconds = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600 }[timeframe] || 60;
  const startSec = Math.floor(sessionStartMs / 1000 / tfSeconds) * tfSeconds;
  const endSec = Math.floor(nowMs / 1000 / tfSeconds) * tfSeconds;
  const scaffold = [];
  for (let ts = startSec; ts <= endSec; ts += tfSeconds) {
    scaffold.push({
      time: ts,
      open: null,
      high: null,
      low: null,
      close: null,
      volume: 0,
      hasTrades: false,
      isPlaceholder: true
    });
  }
  return scaffold;
}

function normalizeKline(row) {
  return {
    time: Math.floor(Number(row[0]) / 1000),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]),
    takerBuyBaseVolume: Number(row[9] || 0),
    hasTrades: Number(row[8] || 0) > 0,
    isPlaceholder: false
  };
}

function buildCvdMinuteCandlesFromKlines(klines = []) {
  const result = [];
  let running = 0;

  klines
    .sort((a, b) => a.time - b.time)
    .forEach((kline) => {
      const totalVolume = Number(kline.volume || 0);
      const buyVolume = Number(kline.takerBuyBaseVolume || 0);
      const sellVolume = Math.max(0, totalVolume - buyVolume);
      const delta = buyVolume - sellVolume;

      const candle = {
        time: kline.time,
        open: running,
        high: running,
        low: running,
        close: running,
        hasTrades: Boolean(kline.hasTrades)
      };

      running += delta;
      candle.high = Math.max(candle.high, running);
      candle.low = Math.min(candle.low, running);
      candle.close = running;
      result.push(candle);
    });

  return result;
}

function mergeMinuteCandlesIntoSession(candles = []) {
  let merged = 0;
  candles.forEach((candle) => {
    if (!candle || !Number.isFinite(candle.time) || candle.time < Math.floor(sessionState.dayStartMs / 1000)) return;
    const existingIndex = sessionState.minuteCandleIndex.get(candle.time);
    if (existingIndex === undefined) {
      sessionState.minuteCandleIndex.set(candle.time, sessionState.minuteCandles.length);
      sessionState.minuteCandles.push(candle);
      merged += 1;
      return;
    }

    sessionState.minuteCandles[existingIndex] = candle;
  });

  sessionState.minuteCandles.sort((a, b) => a.time - b.time);
  sessionState.minuteCandleIndex = new Map(sessionState.minuteCandles.map((candle, index) => [candle.time, index]));
  return merged;
}

async function fetchHistoricalCandlesForDay({ symbol = SYMBOL, date, timeframe = '1m' }) {
  if (timeframe !== '1m') {
    throw new Error(`Unsupported backtest timeframe: ${timeframe}`);
  }

  const dayStartMs = Date.parse(`${date}T00:00:00.000Z`);
  const dayEndMs = dayStartMs + (24 * 60 * 60 * 1000) - 1;
  if (!Number.isFinite(dayStartMs)) {
    throw new Error('Invalid UTC date supplied for historical fetch.');
  }

  const collected = [];
  let cursor = dayStartMs;

  while (cursor <= dayEndMs) {
    const search = new URLSearchParams({
      symbol,
      interval: '1m',
      startTime: String(cursor),
      endTime: String(dayEndMs),
      limit: '1000'
    });

    const { payload: batch } = await fetchBinanceWithFallback('/klines', search, {
      context: 'quant/backtest/klines'
    });

    if (!batch.length) break;

    const normalized = batch.map(normalizeKline);
    collected.push(...normalized);

    const lastOpenMs = Number(batch.at(-1)?.[0] || cursor);
    const nextCursor = lastOpenMs + 60_000;
    if (nextCursor <= cursor) break;
    cursor = nextCursor;
  }

  return collected
    .filter((candle) => candle.time * 1000 >= dayStartMs && candle.time * 1000 <= dayEndMs)
    .sort((a, b) => a.time - b.time);
}

async function backfillCurrentSessionCandlesFromBinance(dayStartMs, nowMs) {
  const collected = [];
  const nowSec = Math.floor(nowMs / 1000) * 1000;
  let cursor = dayStartMs;

  while (cursor <= nowSec) {
    const search = new URLSearchParams({
      symbol: SYMBOL,
      interval: '1m',
      startTime: String(cursor),
      endTime: String(nowSec),
      limit: '1000'
    });

    const { payload: batch } = await fetchBinanceWithFallback('/klines', search, {
      context: 'session/hydration/klines'
    });
    if (!batch.length) break;

    const normalized = batch.map(normalizeKline);
    collected.push(...normalized);

    const lastOpenMs = Number(batch.at(-1)?.[0] || cursor);
    const nextCursor = lastOpenMs + 60_000;
    if (nextCursor <= cursor) break;
    cursor = nextCursor;

  }

  return collected;
}

async function initializeCurrentSession({ reason = 'startup', targetNowMs } = {}) {
  sessionState.hydration = {
    ...sessionState.hydration,
    status: 'running',
    source: 'binance-klines-1m',
    startedAt: Date.now(),
    finishedAt: null,
    lastError: null
  };

  const latestCandle = await fetchLatestAvailableCandle();
  const marketNowMs = Number.isFinite(targetNowMs) ? targetNowMs : (latestCandle.time * 1000) + 59_000;
  const dayStartMs = getUtcDayStartMs(marketNowMs);
  recordMarketTimestamp(latestCandle.time * 1000);
  ensureCurrentSession(marketNowMs);

  console.info('Starting candle backfill', { reason, dayStartIso: new Date(dayStartMs).toISOString() });
  const backfilledCandles = await backfillCurrentSessionCandlesFromBinance(dayStartMs, marketNowMs);
  const mergedCandleCount = mergeMinuteCandlesIntoSession(backfilledCandles);
  const mergedCvdCount = mergeCvdMinuteCandlesIntoSession(buildCvdMinuteCandlesFromKlines(backfilledCandles));
  sessionState.volumeProfile = new Map();
  mergeVolumeProfileIntoSession(backfilledCandles);

  sessionState.hydration = {
    ...sessionState.hydration,
    status: 'complete',
    finishedAt: Date.now(),
    fetchedCandleCount: backfilledCandles.length,
    fetchedTradeCount: 0,
    mergedCandleCount,
    processedTradeCount: 0,
    lastError: null
  };

  const queuedTradeCount = sessionState.pendingDerivedTrades.length;
  const flushedTradeCount = flushPendingDerivedTrades();

  console.info('[session/hydration] complete', {
    reason,
    dayStartIso: new Date(dayStartMs).toISOString(),
    fetchedCandleCount: backfilledCandles.length,
    fetchedTradeCount: 0,
    mergedCandleCount,
    mergedCvdCount,
    processedTradeCount: sessionState.hydration.processedTradeCount,
    inMemoryMinuteCandleCount: sessionState.minuteCandles.length,
    inMemoryCvdMinuteCandleCount: sessionState.cvdMinuteCandles.length,
    queuedTradeCount,
    flushedTradeCount
  });
}

async function initializeCurrentSessionSafe({ reason = 'startup', targetNowMs } = {}) {
  if (sessionInitializationPromise) return sessionInitializationPromise;

  sessionInitializationPromise = (async () => {
    try {
      await initializeCurrentSession({ reason, targetNowMs });
      console.log('Current session initialized.');
    } catch (error) {
      sessionState.hydration = {
        ...sessionState.hydration,
        status: 'failed',
        finishedAt: Date.now(),
        lastError: error?.message || String(error)
      };
      const queuedTradeCount = sessionState.pendingDerivedTrades.length;
      const flushedTradeCount = flushPendingDerivedTrades();
      console.warn('[session/hydration] flushed queued live trades after failure', {
        reason,
        queuedTradeCount,
        flushedTradeCount
      });
      console.error('Session initialization failed; continuing with live streams.', error);
      scheduleSessionInitialization({ reason: 'retry-after-failure', delayMs: SESSION_INITIALIZATION_RETRY_MS });
    } finally {
      sessionInitializationPromise = null;
    }
  })();

  return sessionInitializationPromise;
}

function buildSessionPayload(timeframe = '1m') {
  ensureCurrentSession();
  const nowMs = getEffectiveMarketNowMs();
  const minuteCandles = [...sessionState.minuteCandles];
  const hydratedCandles = aggregateCandles(minuteCandles, timeframe);
  const scaffold = buildTimeScaffold(timeframe, sessionState.dayStartMs, nowMs);
  const hydratedByTime = new Map(hydratedCandles.map((bar) => [bar.time, { ...bar, isPlaceholder: false, state: 'hydrated' }]));
  const candles = scaffold.map((slot) => hydratedByTime.get(slot.time) || { ...slot, state: 'placeholder' });

  const vwap = computeSessionVwapFromCandles(aggregateCandles(minuteCandles, timeframe));
  const cvd = computeSessionCvdFromMinuteCandles(sessionState.cvdMinuteCandles, timeframe, {
    sessionStartMs: sessionState.dayStartMs,
    nowMs
  });

  const timeframeCounts = ['1m', '5m', '15m', '1h'].reduce((acc, tf) => {
    acc[tf] = aggregateCandles(minuteCandles, tf).length;
    return acc;
  }, {});

  const placeholderCount = candles.filter((bar) => bar.isPlaceholder).length;
  const hydratedCount = candles.length - placeholderCount;
  const realOhlcVariance = new Set(hydratedCandles.map((bar) => `${bar.open}:${bar.high}:${bar.low}:${bar.close}`)).size;

  return {
    symbol: SYMBOL,
    timeframe,
    sessionStartMs: sessionState.dayStartMs,
    sessionStartIso: new Date(sessionState.dayStartMs).toISOString(),
    candles,
    vwap,
    cvd,
    debug: {
      sessionTradeCount: sessionState.hydration.processedTradeCount,
      sessionCandleCount: candles.length,
      hydratedCandleCount: hydratedCount,
      placeholderCandleCount: placeholderCount,
      realOhlcVariance,
      timeframeCounts,
      startsAtUtcMidnight: sessionState.dayStartMs === getUtcDayStartMs(nowMs),
      hydration: sessionState.hydration,
      vwapCurrent: vwap.at(-1)?.value || null,
      vwapHasVariance: new Set(vwap.map((point) => point.value.toFixed(8))).size > 1,
      cvdCurrent: cvd.at(-1)?.close || null,
      cvdBarsWithTrades: cvd.filter((bar) => bar.hasTrades).length
    }
  };
}

const strategyUploadService = new StrategyUploadService({
  validationService: new StrategyValidationService(),
  parserService: new StrategyParser(),
  saveStrategyRecord: saveQuantStrategy
});

const strategyParser = new StrategyParser();
const executionEngine = new StrategyExecutionEngine();

function resolveStrategy(strategyRef = {}) {
  if (!strategyRef || typeof strategyRef !== 'object') return null;

  if (strategyRef.kind === 'built_in') {
    const definition = getBuiltInStrategyDefinition(strategyRef.key);
    if (!definition) return null;
    return {
      strategy: definition.strategy,
      summary: {
        id: definition.key,
        name: definition.strategy.metadata.name,
        description: definition.description,
        timeframe: definition.strategy.market.timeframe,
        symbol: definition.strategy.market.symbol,
        entryRules: definition.entryRules,
        exitRules: definition.exitRules,
        source: 'built_in'
      }
    };
  }

  if (strategyRef.kind === 'uploaded') {
    const record = getQuantStrategyById(Number(strategyRef.id));
    if (!record) return null;
    const parsed = strategyParser.parse(record.raw_content);
    if (!parsed.valid) throw new Error(`Strategy invalid: ${parsed.errors.join('; ')}`);
    return {
      strategy: parsed.strategy,
      summary: {
        id: record.id,
        name: parsed.summary.name,
        description: record.parse_message || 'Uploaded JSON strategy.',
        timeframe: parsed.summary.timeframe,
        symbol: parsed.summary.symbol,
        entryRules: { long: 'Uploaded strategy long rule set', short: 'Uploaded strategy short rule set' },
        exitRules: { long: 'Uploaded strategy long exit rules', short: 'Uploaded strategy short exit rules' },
        source: 'uploaded',
        fileName: record.file_name
      }
    };
  }

  return null;
}

function buildStrategyCatalog() {
  const builtIn = listBuiltInLiveStrategies();
  const uploaded = listQuantStrategies(50).map((record) => {
    const metadata = record.metadata_json ? JSON.parse(record.metadata_json) : {};
    return {
      id: record.id,
      key: `uploaded-${record.id}`,
      name: metadata.name || record.file_name,
      label: metadata.name || record.file_name,
      description: record.parse_message || 'Uploaded JSON strategy.',
      symbol: metadata.symbol || SYMBOL,
      timeframe: metadata.timeframe || '1m',
      source: 'uploaded',
      fileName: record.file_name,
      summary: metadata
    };
  });

  return { builtIn, uploaded };
}

function buildLiveMarketSnapshot() {
  const session = buildSessionPayload('1m');
  const vwapByTime = new Map((session.vwap || []).map((point) => [point.time, point.value]));
  const cvdByTime = new Map((session.cvd || []).map((point) => [point.time, point]));
  const rawCandles = (session.candles || [])
    .filter((candle) => !candle.isPlaceholder && Number.isFinite(candle.open) && Number.isFinite(candle.close));
  const candles = enrichMarketCandles(rawCandles, { vwapByTime, cvdByTime });

  const nowMinuteSec = Math.floor(Date.now() / 60000) * 60;
  const closedCandles = candles.filter((candle) => candle.time < nowMinuteSec);
  const latestBid = Number(latestBook?.bid_price ?? latestBook?.bidPrice ?? 0) || null;
  const latestAsk = Number(latestBook?.ask_price ?? latestBook?.askPrice ?? 0) || null;
  const markPrice = Number(latestTrade?.price || latestBid || latestAsk || candles.at(-1)?.close || 0) || null;

  return {
    symbol: SYMBOL,
    bestBid: latestBid,
    bestAsk: latestAsk,
    markPrice,
    lastClose: candles.at(-1)?.close ? Number(candles.at(-1).close) : null,
    analysis: {
      candles,
      closedCandles
    }
  };
}

const liveStrategyRunner = new LivePaperRunner({
  getMarketSnapshot: buildLiveMarketSnapshot,
  saveLiveState: ({ strategyId, status, stateJson }) => saveQuantLiveRun({ strategyId, status, stateJson }),
  getLiveState: () => null,
  strategyResolver: resolveStrategy,
  executionEngine
});

const backtestRunner = new BacktestRunner({
  strategyResolver: resolveStrategy,
  executionEngine,
  fetchDayCandles: async ({ symbol, date, timeframe }) => fetchHistoricalCandlesForDay({ symbol, date, timeframe })
});

const stream = new BinanceStreamService({
  symbol: SYMBOL,
  onTrade: (trade) => {
    latestTrade = trade;
    saveTrade(trade);
    ensureCurrentSession(trade.trade_time);

    applyTradeToMinuteCandle(trade);
    if (sessionState.hydration.status === 'running') {
      queuePendingDerivedTrade(trade);
    } else {
      applyTradeToDerivedState(trade);
    }

    io.emit('trade', trade);
  },
  onBookTicker: (book) => {
    latestBook = book;
    saveBookTicker(book);
    io.emit('bookTicker', book);
  },
  onDepth: (depth) => {
    latestDepth = depth;
    io.emit('depth', depth);
  },
  onCandleBootstrap: (candles) => {
    if (!candles?.length) return;
    const lastBootstrapCandle = candles.at(-1);
    recordMarketTimestamp(lastBootstrapCandle.time * 1000);
    ensureCurrentSession(lastBootstrapCandle.time * 1000);
    mergeMinuteCandlesIntoSession(candles.map((candle) => ({ ...candle, hasTrades: true, isPlaceholder: false })));
    mergeCvdMinuteCandlesIntoSession(buildCvdMinuteCandlesFromKlines(candles));
    mergeVolumeProfileIntoSession(candles);
  },
  onCandle: (candle) => {
    recordMarketTimestamp(candle.time * 1000);
    ensureCurrentSession(candle.time * 1000);
    mergeMinuteCandlesIntoSession([{ ...candle, hasTrades: true, isPlaceholder: false }]);
  },
  onTradeConnected: ({ stream }) => {
    console.info('Connected to Binance trade websocket', { stream });
  }
});

server.listen(PORT, () => {
  console.log(`Kent Invest Crypto Tape Terminal listening on ${PORT}`);
});

stream.start();
initializeCurrentSessionSafe();

io.on('connection', (socket) => {
  const recent = getRecentTrades(SYMBOL, 500).reverse();
  socket.emit('bootstrap', {
    symbol: SYMBOL,
    trades: recent,
    latestTrade: latestTrade || recent.at(-1) || null,
    latestBook: latestBook || getLatestBook(SYMBOL) || null,
    depth: latestDepth,
    sessionStartMs: sessionState.dayStartMs
  });
});

app.get('/api/health', (_req, res) => {
  res.json({ ok: true, symbol: SYMBOL });
});

app.post('/api/quant/strategy/upload', (req, res) => {
  const { fileName, content } = req.body || {};
  const result = strategyUploadService.handleUpload({ fileName, content });
  if (result.status === 'invalid') return res.status(400).json(result);
  return res.json(result);
});

app.get('/api/quant/strategies/catalog', (_req, res) => {
  return res.json({
    strategies: buildStrategyCatalog(),
    limits: LIVE_PAPER_LIMITS
  });
});

app.get('/api/quant/live-metrics', (_req, res) => {
  try {
    const snapshot = liveStrategyRunner.tick() || liveStrategyRunner.getSnapshot();
    return res.json({ snapshot });
  } catch (error) {
    console.error('[api] failed to build live metrics snapshot', error);
    return res.status(500).json({
      error: error.message || 'Unable to load live metrics.',
      snapshot: liveStrategyRunner.getSnapshot?.() || null
    });
  }
});

app.get('/api/quant/live/strategies', (_req, res) => {
  return res.json({
    strategies: buildStrategyCatalog(),
    limits: LIVE_PAPER_LIMITS
  });
});

app.get('/api/quant/backtest/snapshot', async (_req, res) => {
  try {
    return res.json({
      snapshot: backtestRunner.getSnapshot(),
      speeds: Object.values(BACKTEST_SPEEDS),
      limits: LIVE_PAPER_LIMITS,
      suggestedConfig: await buildSuggestedBacktestConfig()
    });
  } catch (error) {
    return res.status(500).json({ error: error.message || 'Unable to load backtest snapshot.' });
  }
});

app.post('/api/quant/backtest/start', async (req, res) => {
  try {
    const { strategyRef, runConfig, startDate, endDate, speed } = req.body || {};
    if (!strategyRef) return res.status(400).json({ error: 'strategyRef is required.' });
    if (!startDate || !endDate) return res.status(400).json({ error: 'startDate and endDate are required.' });

    const normalizedRange = await normalizeBacktestRequestRange({ startDate, endDate });
    const snapshot = backtestRunner.start({
      strategyRef,
      runConfig: runConfig || {},
      startDate: normalizedRange.startDate,
      endDate: normalizedRange.endDate,
      speed
    });

    return res.json({
      snapshot,
      speeds: Object.values(BACKTEST_SPEEDS),
      limits: LIVE_PAPER_LIMITS,
      normalizedRange
    });
  } catch (error) {
    return res.status(400).json({ error: error.message || 'Unable to start backtest.' });
  }
});

app.post('/api/quant/backtest/stop', (_req, res) => {
  return res.json({
    snapshot: backtestRunner.stop(),
    speeds: Object.values(BACKTEST_SPEEDS),
    limits: LIVE_PAPER_LIMITS
  });
});

app.post('/api/quant/live/start', (req, res) => {
  try {
    const { strategyRef, runConfig } = req.body || {};
    if (!strategyRef) return res.status(400).json({ error: 'strategyRef is required.' });
    const run = liveStrategyRunner.start({ strategyRef, runConfig: runConfig || {} });
    return res.json({ run });
  } catch (error) {
    return res.status(400).json({ error: error.message || 'Unable to start live paper strategy.' });
  }
});

app.post('/api/quant/live/stop', (_req, res) => {
  return res.json({ run: liveStrategyRunner.stop() });
});

app.get('/api/session/snapshot', (req, res) => {
  const timeframe = req.query.timeframe || '1m';
  return res.json(buildSessionPayload(timeframe));
});

app.get('/api/candles', (req, res) => {
  const timeframe = req.query.timeframe || '1m';
  const limit = Math.min(Number(req.query.limit || 1440), 2000);
  const payload = buildSessionPayload(timeframe);
  return res.json({ symbol: SYMBOL, timeframe, candles: payload.candles.slice(-limit), sessionStartMs: payload.sessionStartMs });
});

app.get('/api/indicators/vwap', (req, res) => {
  const timeframe = req.query.timeframe || '1m';
  const payload = buildSessionPayload(timeframe);
  return res.json({ symbol: SYMBOL, timeframe, series: payload.vwap, sessionStartMs: payload.sessionStartMs });
});

app.get('/api/indicators/cvd', (req, res) => {
  const timeframe = req.query.timeframe || '1m';
  const payload = buildSessionPayload(timeframe);
  return res.json({ symbol: SYMBOL, timeframe, candles: payload.cvd, sessionStartMs: payload.sessionStartMs });
});

app.get('/api/indicators/volume-profile', (req, res) => {
  const timeframe = req.query.timeframe || '1m';

  ensureCurrentSession();
  const profile = sessionState.volumeProfile.size
    ? buildVolumeProfileFromMap(sessionState.volumeProfile)
    : buildVolumeProfileFromCandles(sessionState.minuteCandles);
  return res.json({
    symbol: SYMBOL,
    timeframe,
    sessionStartMs: sessionState.dayStartMs,
    sessionStartIso: new Date(sessionState.dayStartMs).toISOString(),
    profile,
    poc: profile.reduce((best, bucket) => (bucket.volume > (best?.volume || 0) ? bucket : best), null)
  });
});

app.get('/api/session/debug', (_req, res) => {
  const snapshot = buildSessionPayload('1m');
  return res.json({
    symbol: SYMBOL,
    sessionStartMs: snapshot.sessionStartMs,
    sessionStartIso: snapshot.sessionStartIso,
    sessionTradeCount: snapshot.debug.sessionTradeCount,
    sessionCandleCount1m: snapshot.debug.sessionCandleCount,
    startsAtUtcMidnight: snapshot.debug.startsAtUtcMidnight,
    latestVwap: snapshot.debug.vwapCurrent,
    latestCvd: snapshot.debug.cvdCurrent,
    timeframeCounts: snapshot.debug.timeframeCounts,
    hydratedCandleCount: snapshot.debug.hydratedCandleCount,
    placeholderCandleCount: snapshot.debug.placeholderCandleCount,
    realOhlcVariance: snapshot.debug.realOhlcVariance,
    hydration: snapshot.debug.hydration,
    vwapHasVariance: snapshot.debug.vwapHasVariance,
    cvdBarsWithTrades: snapshot.debug.cvdBarsWithTrades
  });
});

app.use('/api', (_req, res) => {
  return res.status(404).json({ error: 'API route not found.' });
});

app.use((error, req, res, next) => {
  if (!req.path.startsWith('/api')) return next(error);
  console.error('[api] unhandled error', error);
  if (res.headersSent) return next(error);
  return res.status(error.status || 500).json({
    error: error.message || 'Internal server error.'
  });
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const clientDist = path.resolve(__dirname, '../client/dist');

app.use(express.static(clientDist));
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api')) return next();
  res.sendFile(path.join(clientDist, 'index.html'));
});

const TIMEFRAME_TO_SECONDS = {
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '1h': 3600
};

export function timeframeToSeconds(timeframe = '1m') {
  return TIMEFRAME_TO_SECONDS[timeframe] || TIMEFRAME_TO_SECONDS['1m'];
}

export function getUtcDayStartMs(ts = Date.now()) {
  const d = new Date(ts);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0);
}

export function bucketTime(unixSeconds, timeframe = '1m') {
  const sec = timeframeToSeconds(timeframe);
  return Math.floor(unixSeconds / sec) * sec;
}

function sortTrades(trades) {
  return [...trades].sort((a, b) => a.trade_time - b.trade_time || a.trade_id - b.trade_id);
}

export function buildCanonicalMinuteCandles(
  trades,
  {
    sessionStartMs,
    nowMs = Date.now(),
    includeEmptyMinutes = true,
    carryForwardOnEmpty = true
  } = {}
) {
  const ordered = sortTrades(trades);
  const resolvedSessionStartMs = sessionStartMs ?? getUtcDayStartMs(nowMs);
  const startSec = Math.floor(resolvedSessionStartMs / 1000);
  const endSec = bucketTime(Math.floor(nowMs / 1000), '1m');

  const minuteMap = new Map();
  ordered.forEach((trade) => {
    if (trade.trade_time < resolvedSessionStartMs || trade.trade_time > nowMs) return;
    const time = bucketTime(Math.floor(trade.trade_time / 1000), '1m');
    const price = Number(trade.price);
    const volume = Number(trade.quantity || 0);

    const existing = minuteMap.get(time);
    if (!existing) {
      minuteMap.set(time, {
        time,
        open: price,
        high: price,
        low: price,
        close: price,
        volume,
        hasTrades: true
      });
      return;
    }

    existing.high = Math.max(existing.high, price);
    existing.low = Math.min(existing.low, price);
    existing.close = price;
    existing.volume += volume;
    existing.hasTrades = true;
  });

  if (!includeEmptyMinutes) {
    return [...minuteMap.values()].sort((a, b) => a.time - b.time);
  }

  const candles = [];
  let lastClose = ordered.length ? Number(ordered[0].price) : 0;
  for (let ts = startSec; ts <= endSec; ts += 60) {
    const existing = minuteMap.get(ts);
    if (existing) {
      lastClose = existing.close;
      candles.push(existing);
    } else if (carryForwardOnEmpty) {
      candles.push({
        time: ts,
        open: lastClose,
        high: lastClose,
        low: lastClose,
        close: lastClose,
        volume: 0,
        hasTrades: false
      });
    } else {
      candles.push({
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
  }

  return candles;
}

export function aggregateCandles(candles, timeframe = '1m') {
  const hydratedOnly = candles.filter((candle) => Number.isFinite(candle.open) && Number.isFinite(candle.close));

  if (timeframe === '1m') {
    return hydratedOnly.map(({ time, open, high, low, close, volume, hasTrades }) => ({ time, open, high, low, close, volume, hasTrades }));
  }

  const tfSec = timeframeToSeconds(timeframe);
  const buckets = new Map();

  hydratedOnly.forEach((candle) => {
    const bucket = bucketTime(candle.time, timeframe);
    const existing = buckets.get(bucket);
    if (!existing) {
      buckets.set(bucket, {
        time: bucket,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: Number(candle.volume || 0),
        hasTrades: Boolean(candle.hasTrades)
      });
      return;
    }

    existing.high = Math.max(existing.high, candle.high);
    existing.low = Math.min(existing.low, candle.low);
    existing.close = candle.close;
    existing.volume += Number(candle.volume || 0);
    existing.hasTrades = existing.hasTrades || Boolean(candle.hasTrades);
  });

  const aggregated = [...buckets.values()].sort((a, b) => a.time - b.time);
  for (let i = 1; i < aggregated.length; i += 1) {
    if (!aggregated[i].hasTrades) {
      const prevClose = aggregated[i - 1].close;
      aggregated[i].open = prevClose;
      aggregated[i].high = prevClose;
      aggregated[i].low = prevClose;
      aggregated[i].close = prevClose;
    }
  }

  return aggregated;
}

export function buildCandlesFromTrades(trades, timeframe = '1m', options = {}) {
  const minuteCandles = buildCanonicalMinuteCandles(trades, options);
  return aggregateCandles(minuteCandles, timeframe);
}

export function computeSessionVwapFromCandles(candles) {
  const running = [];
  let cumulativePv = 0;
  let cumulativeVolume = 0;

  candles.forEach((candle) => {
    const typicalPrice = (candle.high + candle.low + candle.close) / 3;
    const volume = Number(candle.volume || 0);
    cumulativePv += typicalPrice * volume;
    cumulativeVolume += volume;

    running.push({
      time: candle.time,
      value: cumulativeVolume > 0 ? cumulativePv / cumulativeVolume : candle.close
    });
  });

  return running;
}

export function computeSessionVwapFromTrades(trades, timeframe = '1m', options = {}) {
  const candles = buildCandlesFromTrades(trades, timeframe, options);
  return computeSessionVwapFromCandles(candles);
}

export function computeSessionCvdFromTrades(trades, timeframe = '1m', { sessionStartMs, nowMs = Date.now() } = {}) {
  const ordered = sortTrades(trades);
  const resolvedSessionStartMs = sessionStartMs ?? getUtcDayStartMs(nowMs);
  const startSec = Math.floor(resolvedSessionStartMs / 1000);
  const endSec = bucketTime(Math.floor(nowMs / 1000), timeframe);

  const buckets = new Map();
  let running = 0;

  ordered.forEach((trade) => {
    if (trade.trade_time < resolvedSessionStartMs || trade.trade_time > nowMs) return;
    const candleTime = bucketTime(Math.floor(trade.trade_time / 1000), timeframe);
    if (!buckets.has(candleTime)) {
      buckets.set(candleTime, {
        time: candleTime,
        open: running,
        high: running,
        low: running,
        close: running,
        hasTrades: false
      });
    }

    const candle = buckets.get(candleTime);
    const delta = Number(trade.maker_flag) ? -Number(trade.quantity || 0) : Number(trade.quantity || 0);
    running += delta;
    candle.high = Math.max(candle.high, running);
    candle.low = Math.min(candle.low, running);
    candle.close = running;
    candle.hasTrades = true;
  });

  const tfSec = timeframeToSeconds(timeframe);
  const result = [];
  let previousClose = 0;
  for (let ts = bucketTime(startSec, timeframe); ts <= endSec; ts += tfSec) {
    const existing = buckets.get(ts);
    if (existing) {
      previousClose = existing.close;
      result.push(existing);
    } else {
      result.push({ time: ts, open: previousClose, high: previousClose, low: previousClose, close: previousClose, hasTrades: false });
    }
  }

  return result;
}

export function computeSessionCvdFromMinuteCandles(minuteCandles = [], timeframe = '1m', { sessionStartMs, nowMs = Date.now() } = {}) {
  const resolvedSessionStartMs = sessionStartMs ?? getUtcDayStartMs(nowMs);
  const startSec = bucketTime(Math.floor(resolvedSessionStartMs / 1000), timeframe);
  const endSec = bucketTime(Math.floor(nowMs / 1000), timeframe);
  const tfSec = timeframeToSeconds(timeframe);

  if (timeframe === '1m') {
    const minuteMap = new Map(minuteCandles.map((candle) => [candle.time, candle]));
    const result = [];
    let previousClose = 0;

    for (let ts = startSec; ts <= endSec; ts += tfSec) {
      const existing = minuteMap.get(ts);
      if (existing) {
        previousClose = existing.close;
        result.push({ ...existing });
      } else {
        result.push({ time: ts, open: previousClose, high: previousClose, low: previousClose, close: previousClose, hasTrades: false });
      }
    }

    return result;
  }

  const minuteSeries = computeSessionCvdFromMinuteCandles(minuteCandles, '1m', { sessionStartMs: resolvedSessionStartMs, nowMs });
  const buckets = new Map();

  minuteSeries.forEach((candle) => {
    const bucket = bucketTime(candle.time, timeframe);
    const existing = buckets.get(bucket);
    if (!existing) {
      buckets.set(bucket, {
        time: bucket,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        hasTrades: Boolean(candle.hasTrades)
      });
      return;
    }

    existing.high = Math.max(existing.high, candle.high);
    existing.low = Math.min(existing.low, candle.low);
    existing.close = candle.close;
    existing.hasTrades = existing.hasTrades || Boolean(candle.hasTrades);
  });

  const result = [];
  let previousClose = 0;
  for (let ts = startSec; ts <= endSec; ts += tfSec) {
    const existing = buckets.get(ts);
    if (existing) {
      previousClose = existing.close;
      result.push(existing);
    } else {
      result.push({ time: ts, open: previousClose, high: previousClose, low: previousClose, close: previousClose, hasTrades: false });
    }
  }

  return result;
}

function normalizeVolumeProfile(volumeMap = new Map()) {
  if (!volumeMap.size) return [];

  const sorted = [...volumeMap.entries()]
    .filter(([price, volume]) => Number.isFinite(price) && Number.isFinite(volume) && volume > 0)
    .sort((a, b) => a[0] - b[0]);

  if (!sorted.length) return [];

  const maxVolume = Math.max(...sorted.map(([, volume]) => volume), 1);

  return sorted.map(([price, volume]) => ({
    price,
    volume,
    ratio: volume / maxVolume
  }));
}

export function buildVolumeProfileByDollar(trades = []) {
  const buckets = new Map();

  trades.forEach((trade) => {
    const price = Math.floor(Number(trade.price));
    const volume = Number(trade.quantity || 0);
    if (!Number.isFinite(price) || !Number.isFinite(volume) || volume <= 0) return;

    buckets.set(price, (buckets.get(price) || 0) + volume);
  });

  return normalizeVolumeProfile(buckets);
}

export function buildVolumeProfileFromCandles(candles = []) {
  const buckets = new Map();

  candles.forEach((candle) => {
    const low = Math.floor(Number(candle.low));
    const high = Math.floor(Number(candle.high));
    const volume = Number(candle.volume || 0);
    if (!Number.isFinite(low) || !Number.isFinite(high) || !Number.isFinite(volume) || volume <= 0) return;

    const start = Math.min(low, high);
    const end = Math.max(low, high);
    const bucketCount = Math.max(end - start + 1, 1);
    const volumePerBucket = volume / bucketCount;

    for (let price = start; price <= end; price += 1) {
      buckets.set(price, (buckets.get(price) || 0) + volumePerBucket);
    }
  });

  return normalizeVolumeProfile(buckets);
}

export function buildVolumeProfileFromMap(volumeMap = new Map()) {
  return normalizeVolumeProfile(volumeMap);
}

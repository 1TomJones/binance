import { bucketTime, timeframeToSeconds } from '../sessionAnalytics.js';

export class HistoricalMarketReplay {
  constructor({
    timeframe = '1m',
    sessionStartMs,
    sessionEndMs,
    seedPrice = null,
    settings = {}
  }) {
    this.timeframe = timeframe;
    this.timeframeSec = timeframeToSeconds(timeframe);
    this.sessionStartSec = bucketTime(Math.floor(sessionStartMs / 1000), timeframe);
    this.sessionEndSec = bucketTime(Math.floor(sessionEndMs / 1000), timeframe);
    this.seedPrice = toFiniteNumber(seedPrice);
    this.settings = settings;

    this.currentBucketSec = this.seedPrice != null ? this.sessionStartSec : null;
    this.currentBucket = null;
    this.lastClose = this.seedPrice;
    this.cumulativePriceVolume = 0;
    this.cumulativeVolume = 0;
    this.cvdRunning = 0;
    this.recentVolumes = [];
    this.emittedCandles = 0;
  }

  processTrade(trade) {
    if (!trade) return [];

    const tradeTimeSec = Math.floor(Number(trade.trade_time || 0) / 1000);
    const tradeBucketSec = bucketTime(tradeTimeSec, this.timeframe);
    if (tradeBucketSec < this.sessionStartSec || tradeBucketSec > this.sessionEndSec) {
      return [];
    }

    if (this.currentBucketSec == null) {
      this.currentBucketSec = tradeBucketSec;
    }

    const emitted = [];
    while (this.currentBucketSec < tradeBucketSec) {
      const closed = this.#finalizeCurrentBucket();
      if (closed) emitted.push(closed);
      this.currentBucketSec += this.timeframeSec;
    }

    if (!this.currentBucket) {
      this.currentBucket = this.#createBucket(this.currentBucketSec);
    }

    this.#applyTrade(trade);
    return emitted;
  }

  flushRemaining() {
    const emitted = [];

    if (this.currentBucketSec == null) {
      if (this.seedPrice == null) return emitted;
      this.currentBucketSec = this.sessionStartSec;
    }

    while (this.currentBucketSec <= this.sessionEndSec) {
      const closed = this.#finalizeCurrentBucket();
      if (closed) emitted.push(closed);
      this.currentBucketSec += this.timeframeSec;
    }

    return emitted;
  }

  #createBucket(time) {
    const anchorPrice = this.lastClose ?? this.seedPrice;
    return {
      time,
      open: anchorPrice,
      high: anchorPrice,
      low: anchorPrice,
      close: anchorPrice,
      volume: 0,
      buyVolume: 0,
      sellVolume: 0,
      hasTrades: false,
      cvdOpen: this.cvdRunning,
      cvdHigh: this.cvdRunning,
      cvdLow: this.cvdRunning,
      cvdClose: this.cvdRunning
    };
  }

  #applyTrade(trade) {
    const price = toFiniteNumber(trade.price);
    const quantity = Math.max(toFiniteNumber(trade.quantity) || 0, 0);
    if (price == null || quantity <= 0) return;

    if (!this.currentBucket) {
      this.currentBucket = this.#createBucket(this.currentBucketSec ?? this.sessionStartSec);
    }

    const bucket = this.currentBucket;
    if (!bucket.hasTrades) {
      bucket.open = price;
      bucket.high = price;
      bucket.low = price;
      bucket.close = price;
    } else {
      bucket.high = Math.max(bucket.high, price);
      bucket.low = Math.min(bucket.low, price);
      bucket.close = price;
    }

    bucket.volume += quantity;
    bucket.hasTrades = true;

    if (String(trade.side).toLowerCase() === 'buy') {
      bucket.buyVolume += quantity;
    } else {
      bucket.sellVolume += quantity;
    }

    const cvdDelta = Number(trade.maker_flag) ? -quantity : quantity;
    this.cvdRunning += cvdDelta;
    bucket.cvdHigh = Math.max(bucket.cvdHigh, this.cvdRunning);
    bucket.cvdLow = Math.min(bucket.cvdLow, this.cvdRunning);
    bucket.cvdClose = this.cvdRunning;
  }

  #finalizeCurrentBucket() {
    let bucket = this.currentBucket;

    if (!bucket) {
      if (this.lastClose == null) return null;
      bucket = this.#createBucket(this.currentBucketSec);
    }

    if (!bucket.hasTrades) {
      if (this.lastClose == null) return null;
      bucket.open = this.lastClose;
      bucket.high = this.lastClose;
      bucket.low = this.lastClose;
      bucket.close = this.lastClose;
      bucket.cvdHigh = Math.max(bucket.cvdHigh, this.cvdRunning);
      bucket.cvdLow = Math.min(bucket.cvdLow, this.cvdRunning);
      bucket.cvdClose = this.cvdRunning;
    }

    const typicalPrice = (bucket.high + bucket.low + bucket.close) / 3;
    this.cumulativePriceVolume += typicalPrice * bucket.volume;
    this.cumulativeVolume += bucket.volume;
    this.lastClose = bucket.close;

    this.recentVolumes = [...this.recentVolumes, bucket.volume].slice(-20);
    const avgVolume20 = this.recentVolumes.reduce((sum, value) => sum + value, 0) / Math.max(this.recentVolumes.length, 1);
    const sessionVwap = this.cumulativeVolume > 0
      ? this.cumulativePriceVolume / this.cumulativeVolume
      : bucket.close;

    this.currentBucket = null;
    this.emittedCandles += 1;

    return {
      time: bucket.time,
      open: round(bucket.open),
      high: round(bucket.high),
      low: round(bucket.low),
      close: round(bucket.close),
      volume: round(bucket.volume),
      hasTrades: bucket.hasTrades,
      vwap: round(sessionVwap),
      vwap_session: round(sessionVwap),
      cvd_open: round(bucket.cvdOpen),
      cvd_high: round(bucket.cvdHigh),
      cvd_low: round(bucket.cvdLow),
      cvd_close: round(bucket.cvdClose),
      dom_visible_buy_limits: round(bucket.buyVolume),
      dom_visible_sell_limits: round(bucket.sellVolume),
      avg_volume_20: round(avgVolume20),
      stopLossPct: toFiniteNumber(this.settings.stopLossPct) ?? 0.35,
      takeProfitPct: toFiniteNumber(this.settings.takeProfitPct) ?? 0.7
    };
  }
}

function toFiniteNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function round(value) {
  return Number((value || 0).toFixed(6));
}

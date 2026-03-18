import {
  buildCandlesFromTrades,
  computeSessionCvdFromTrades,
  computeSessionVwapFromTrades,
  timeframeToSeconds
} from '../sessionAnalytics.js';

export function enrichCandlesFromTrades(trades, timeframe, settings, { sessionStartMs, nowMs } = {}) {
  const candles = buildCandlesFromTrades(trades, timeframe, { sessionStartMs, nowMs });
  const vwap = new Map(computeSessionVwapFromTrades(trades, timeframe, { sessionStartMs, nowMs }).map((x) => [x.time, x.value]));
  const cvd = new Map(computeSessionCvdFromTrades(trades, timeframe, { sessionStartMs, nowMs }).map((x) => [x.time, x]));
  const byBucket = new Map();

  const tfSec = timeframeToSeconds(timeframe);
  for (const trade of trades) {
    if (sessionStartMs != null && trade.trade_time < sessionStartMs) continue;
    if (nowMs != null && trade.trade_time > nowMs) continue;

    const time = Math.floor((trade.trade_time / 1000) / tfSec) * tfSec;
    const bucket = byBucket.get(time) || { buy: 0, sell: 0 };
    if (trade.side === 'buy') bucket.buy += Number(trade.quantity || 0);
    else bucket.sell += Number(trade.quantity || 0);
    byBucket.set(time, bucket);
  }

  return enrichMarketCandles(candles, { vwapByTime: vwap, cvdByTime: cvd, byBucket, settings });
}

export function enrichMarketCandles(candles, { vwapByTime, cvdByTime, byBucket = new Map(), settings = {} } = {}) {
  return (candles || []).map((candle, idx, arr) => {
    const bucket = byBucket.get(candle.time) || { buy: 0, sell: 0 };
    const recent = arr.slice(Math.max(0, idx - 19), idx + 1);
    const avgVolume20 = recent.reduce((acc, x) => acc + Number(x.volume || 0), 0) / Math.max(recent.length, 1);
    const cvdCandle = cvdByTime?.get(candle.time) || { open: 0, high: 0, low: 0, close: 0 };
    const previous = arr[idx - 1] || candle;
    const previousCvd = cvdByTime?.get(previous.time) || cvdCandle;
    const sessionVwap = vwapByTime?.get(candle.time) ?? candle.close;

    return {
      ...candle,
      vwap: sessionVwap,
      vwap_session: sessionVwap,
      cvd_open: cvdCandle.open ?? 0,
      cvd_high: cvdCandle.high ?? 0,
      cvd_low: cvdCandle.low ?? 0,
      cvd_close: cvdCandle.close ?? 0,
      prev_cvd_close: previousCvd.close ?? cvdCandle.close ?? 0,
      dom_visible_buy_limits: bucket.buy,
      dom_visible_sell_limits: bucket.sell,
      avg_volume_20: avgVolume20,
      stopLossPct: settings.stopLossPct,
      takeProfitPct: settings.takeProfitPct
    };
  });
}

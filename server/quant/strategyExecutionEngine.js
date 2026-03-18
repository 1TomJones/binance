import { RuleEvaluator } from './ruleEvaluator.js';
import { MetricsCalculator } from './metricsCalculator.js';

export const PAPER_EXECUTION_LIMITS = {
  orderSizeMin: 0.0001,
  orderSizeMax: 0.005,
  orderSizeStep: 0.0001,
  initialBalance: 10000,
  maxReplaySpeed: 60
};

const DEFAULT_SYNTHETIC_SPREAD_BPS = 0.75;

export class StrategyExecutionEngine {
  constructor({ ruleEvaluator = new RuleEvaluator(), metricsCalculator = new MetricsCalculator() } = {}) {
    this.ruleEvaluator = ruleEvaluator;
    this.metricsCalculator = metricsCalculator;
  }

  createRunState({ strategy, runConfig = {} } = {}) {
    const initialBalance = Number(runConfig.initialBalance || strategy.backtestDefaults?.initial_balance || PAPER_EXECUTION_LIMITS.initialBalance);
    const orderSize = normalizeOrderSize(runConfig.orderSize);

    return {
      initialBalance,
      equity: initialBalance,
      peakEquity: initialBalance,
      orderSize,
      settings: {
        stopLossPct: Number(runConfig.stopLossPct ?? strategy.risk?.stop_loss_pct ?? 0.35),
        takeProfitPct: Number(runConfig.takeProfitPct ?? strategy.risk?.take_profit_pct ?? 0.7),
        enableLong: runConfig.enableLong ?? strategy.market?.allow_long ?? true,
        enableShort: runConfig.enableShort ?? strategy.market?.allow_short ?? true,
        syntheticSpreadBps: Number(runConfig.syntheticSpreadBps || DEFAULT_SYNTHETIC_SPREAD_BPS)
      },
      trades: [],
      tradeLog: [],
      equitySeries: [],
      cumulativeRealizedSeries: [],
      position: null,
      lastProcessedCandleTime: null,
      session: this.createSessionState()
    };
  }

  createSessionState() {
    return {
      candles: [],
      previousCandle: null,
      cooldownBars: 0,
      dayTradeCount: 0,
      sessionDate: null
    };
  }

  processCandle({ strategy, state, candle, fillModel, currentDateLabel }) {
    const previousCandle = state.session.previousCandle || candle;
    const context = this.#buildContext(candle, previousCandle, state.position, strategy);

    if (state.position) {
      state.position.holdingBars += 1;
      if (strategy.positionManagement?.enable_break_even && !state.position.breakEvenMoved) {
        const profitPct = this.#positionPnlPct(state.position, candle.close);
        if (profitPct >= Number(strategy.positionManagement.move_stop_to_break_even_at_profit_pct || 0)) {
          state.position.stopPrice = state.position.entryPrice;
          state.position.breakEvenMoved = true;
        }
      }

      const sideExitRules = state.position.side === 'long' ? strategy.exitRules.long : strategy.exitRules.short;
      const exitReason = this.#resolveExitReason({ strategy, position: state.position, sideExitRules, context, candle });
      if (exitReason) {
        const exitPrice = fillModel.getExitPrice({ side: state.position.side, candle, reason: exitReason });
        const closed = this.#closePosition({ position: state.position, candle, exitPrice, reason: exitReason });
        state.equity += closed.realizedPnl;
        state.trades.push(closed);
        state.cumulativeRealizedSeries.push({
          index: state.cumulativeRealizedSeries.length + 1,
          time: closed.exitTime,
          cumulativeRealizedPnl: round(state.trades.reduce((sum, trade) => sum + trade.realizedPnl, 0))
        });
        state.tradeLog.unshift(buildTradeLogRow(closed, 'EXIT'));
        state.position = null;
        state.session.cooldownBars = Number(strategy.execution?.cooldown_bars_after_exit || 0);
        state.session.dayTradeCount += 1;
      }
    }

    if (!state.position && state.session.cooldownBars > 0) {
      state.session.cooldownBars -= 1;
    }

    if (!state.position && state.session.cooldownBars === 0) {
      const longSignal = state.settings.enableLong && strategy.market.allow_long && this.ruleEvaluator.evaluateBlock(strategy.entryRules.long, context);
      const shortSignal = state.settings.enableShort && strategy.market.allow_short && this.ruleEvaluator.evaluateBlock(strategy.entryRules.short, context);

      if (longSignal) {
        const entryPrice = fillModel.getEntryPrice({ side: 'long', candle });
        state.position = this.#openPosition({ side: 'long', candle, entryPrice, quantity: state.orderSize, signalReason: 'Long signal confirmed.' });
        state.tradeLog.unshift(buildTradeLogRow(state.position, 'BUY'));
      } else if (shortSignal) {
        const entryPrice = fillModel.getEntryPrice({ side: 'short', candle });
        state.position = this.#openPosition({ side: 'short', candle, entryPrice, quantity: state.orderSize, signalReason: 'Short signal confirmed.' });
        state.tradeLog.unshift(buildTradeLogRow(state.position, 'SELL'));
      }
    }

    const drawdownPct = this.#pushEquityPoint(state, candle.time, currentDateLabel);
    state.lastProcessedCandleTime = candle.time;
    state.session.candles.push(candle);
    state.session.previousCandle = candle;

    return {
      equity: round(state.equity),
      drawdownPct,
      tradeCount: state.trades.length,
      openPosition: state.position,
      lastProcessedCandleTime: state.lastProcessedCandleTime
    };
  }

  finalizeDay({ strategy, state, fillModel, dateLabel }) {
    if (!state.position) {
      state.session = this.createSessionState();
      state.session.sessionDate = dateLabel;
      return null;
    }

    const finalCandle = state.session.previousCandle;
    const exitPrice = fillModel.getExitPrice({ side: state.position.side, candle: finalCandle, reason: 'end_of_day_exit' });
    const closed = this.#closePosition({
      position: state.position,
      candle: finalCandle,
      exitPrice,
      reason: 'end_of_day_exit'
    });

    state.equity += closed.realizedPnl;
    state.trades.push(closed);
    state.cumulativeRealizedSeries.push({
      index: state.cumulativeRealizedSeries.length + 1,
      time: closed.exitTime,
      cumulativeRealizedPnl: round(state.trades.reduce((sum, trade) => sum + trade.realizedPnl, 0))
    });
    state.tradeLog.unshift(buildTradeLogRow(closed, 'EXIT'));
    state.position = null;
    this.#pushEquityPoint(state, finalCandle.time, dateLabel);
    state.session = this.createSessionState();
    state.session.sessionDate = dateLabel;
    return closed;
  }

  finalizeRun({ strategy, state, lastPrice = null }) {
    const metrics = this.metricsCalculator.calculate({
      initialBalance: state.initialBalance,
      equitySeries: state.equitySeries,
      trades: state.trades,
      openPosition: state.position,
      lastPrice,
      cumulativeRealizedSeries: state.cumulativeRealizedSeries
    });

    return {
      metrics,
      equitySeries: state.equitySeries,
      drawdownSeries: state.equitySeries.map((point) => ({ time: point.time, drawdownPct: point.drawdownPct })),
      trades: state.trades,
      tradeLog: state.tradeLog,
      cumulativePnlSeries: state.cumulativeRealizedSeries,
      endingBalance: metrics.currentEquity,
      initialBalance: state.initialBalance,
      analyses: this.metricsCalculator.buildAnalyses({ trades: state.trades })
    };
  }

  createFillModel({ syntheticSpreadBps = DEFAULT_SYNTHETIC_SPREAD_BPS, quoteResolver } = {}) {
    return {
      getEntryPrice: ({ side, candle }) => {
        const quote = quoteResolver?.(candle) || buildSyntheticQuote(candle, syntheticSpreadBps);
        return side === 'long' ? quote.ask : quote.bid;
      },
      getExitPrice: ({ side, candle }) => {
        const quote = quoteResolver?.(candle) || buildSyntheticQuote(candle, syntheticSpreadBps);
        return side === 'long' ? quote.bid : quote.ask;
      }
    };
  }

  #pushEquityPoint(state, candleTime, currentDateLabel) {
    state.peakEquity = Math.max(state.peakEquity, state.equity);
    const drawdownPct = state.peakEquity ? ((state.peakEquity - state.equity) / state.peakEquity) * 100 : 0;
    state.equitySeries.push({
      time: candleTime,
      equity: round(state.equity),
      drawdownPct: round(drawdownPct),
      date: currentDateLabel
    });

    return round(drawdownPct);
  }

  #buildContext(candle, previousCandle, position, strategy) {
    const values = {
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      prev_close: previousCandle.close,
      prev_high: previousCandle.high,
      prev_low: previousCandle.low,
      prev_volume: previousCandle.volume,
      vwap_session: candle.vwap_session,
      cvd_open: candle.cvd_open,
      cvd_high: candle.cvd_high,
      cvd_low: candle.cvd_low,
      cvd_close: candle.cvd_close,
      prev_cvd_close: previousCandle.cvd_close,
      dom_visible_buy_limits: candle.dom_visible_buy_limits,
      dom_visible_sell_limits: candle.dom_visible_sell_limits,
      avg_volume_20: candle.avg_volume_20
    };

    const builtin = {
      stop_loss: false,
      take_profit: false,
      max_holding_bars: false
    };

    if (position) {
      if (position.side === 'long') {
        builtin.stop_loss = candle.close <= position.stopPrice;
        builtin.take_profit = candle.close >= position.takeProfitPrice;
      } else {
        builtin.stop_loss = candle.close >= position.stopPrice;
        builtin.take_profit = candle.close <= position.takeProfitPrice;
      }
      builtin.max_holding_bars = position.holdingBars >= Number(strategy.risk?.max_holding_bars || 1);
    }

    return { values, builtin };
  }

  #resolveExitReason({ position, sideExitRules, context }) {
    const wrapper = sideExitRules?.all ? 'all' : 'any';
    const conditions = sideExitRules?.[wrapper] || [];
    const evaluations = conditions.map((condition) => ({
      condition,
      matched: this.ruleEvaluator.evaluateCondition(condition, context)
    }));

    const triggered = wrapper === 'all'
      ? evaluations.length > 0 && evaluations.every((entry) => entry.matched)
      : evaluations.some((entry) => entry.matched);

    if (!triggered) return null;
    const builtinMatch = evaluations.find((entry) => entry.matched && entry.condition.type);
    return builtinMatch?.condition.type || 'signal_exit';
  }

  #openPosition({ side, candle, entryPrice, quantity, signalReason }) {
    return {
      status: 'open',
      side,
      quantity: normalizeOrderSize(quantity),
      entryTime: candle.time * 1000,
      entryCandleTime: candle.time,
      entryPrice: round(entryPrice),
      entryReason: signalReason,
      entryDate: new Date(candle.time * 1000).toISOString().slice(0, 10),
      holdingBars: 0,
      breakEvenMoved: false,
      stopPrice: side === 'long'
        ? round(entryPrice * (1 - Number(candle.stopLossPct ?? 0.35) / 100))
        : round(entryPrice * (1 + Number(candle.stopLossPct ?? 0.35) / 100)),
      takeProfitPrice: side === 'long'
        ? round(entryPrice * (1 + Number(candle.takeProfitPct ?? 0.7) / 100))
        : round(entryPrice * (1 - Number(candle.takeProfitPct ?? 0.7) / 100))
    };
  }

  #closePosition({ position, candle, exitPrice, reason }) {
    const realizedPnl = position.side === 'long'
      ? (exitPrice - position.entryPrice) * position.quantity
      : (position.entryPrice - exitPrice) * position.quantity;

    return {
      ...position,
      status: 'closed',
      exitTime: candle.time * 1000,
      exitPrice: round(exitPrice),
      realizedPnl: round(realizedPnl),
      returnPct: round((realizedPnl / Math.max(position.entryPrice * position.quantity, 1e-9)) * 100),
      exitReason: reason,
      holdingBars: position.holdingBars,
      durationMs: Math.max(candle.time * 1000 - position.entryTime, 60000),
      durationMinutes: Math.max((candle.time * 1000 - position.entryTime) / 60000, 1)
    };
  }

  #positionPnlPct(position, markPrice) {
    const pnl = position.side === 'long'
      ? (markPrice - position.entryPrice) * position.quantity
      : (position.entryPrice - markPrice) * position.quantity;
    return (pnl / Math.max(position.entryPrice * position.quantity, 1e-9)) * 100;
  }
}

function buildSyntheticQuote(candle, syntheticSpreadBps) {
  const mid = Number(candle.close || candle.open || 0);
  const halfSpread = mid * (syntheticSpreadBps / 10000 / 2);
  return {
    bid: round(mid - halfSpread),
    ask: round(mid + halfSpread)
  };
}

function buildTradeLogRow(trade, action) {
  return {
    id: `${action}-${trade.entryTime}-${trade.exitTime || trade.entryTime}`,
    timestamp: action === 'EXIT' ? trade.exitTime : trade.entryTime,
    action,
    side: trade.side,
    size: trade.quantity,
    fillPrice: action === 'EXIT' ? trade.exitPrice : trade.entryPrice,
    reason: action === 'EXIT' ? trade.exitReason : trade.entryReason,
    resultingPosition: action === 'EXIT' ? 'Flat' : `${trade.side} ${trade.quantity.toFixed(4)}`,
    realizedPnl: action === 'EXIT' ? trade.realizedPnl : null
  };
}

function normalizeOrderSize(value) {
  const numeric = Number(value || PAPER_EXECUTION_LIMITS.orderSizeMin);
  const clamped = Math.min(PAPER_EXECUTION_LIMITS.orderSizeMax, Math.max(PAPER_EXECUTION_LIMITS.orderSizeMin, numeric));
  const steps = Math.round(clamped / PAPER_EXECUTION_LIMITS.orderSizeStep);
  return Number((steps * PAPER_EXECUTION_LIMITS.orderSizeStep).toFixed(4));
}

function round(value) {
  return Number((value || 0).toFixed(6));
}

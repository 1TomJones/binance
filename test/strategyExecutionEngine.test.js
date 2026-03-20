import test from 'node:test';
import assert from 'node:assert/strict';

import { StrategyExecutionEngine } from '../server/quant/strategyExecutionEngine.js';

function buildCandle(time, { open, high, low, close }) {
  return {
    time,
    open,
    high,
    low,
    close,
    volume: 1,
    takerBuyBaseVolume: 0.5,
    vwap_session: close,
    cvd_open: 0,
    cvd_high: 0,
    cvd_low: 0,
    cvd_close: 0,
    dom_visible_buy_limits: 0,
    dom_visible_sell_limits: 0,
    avg_volume_20: 1,
    stopLossPct: 0.35,
    takeProfitPct: 0.7
  };
}

function buildStrategy() {
  return {
    market: {
      allow_long: true,
      allow_short: false
    },
    risk: {
      max_holding_bars: 1
    },
    execution: {
      cooldown_bars_after_exit: 0
    },
    entryRules: {
      long: {
        any: [{ left: 'close', operator: 'gt', right: 'prev_close' }]
      },
      short: {
        any: []
      }
    },
    exitRules: {
      long: {
        any: [{ type: 'max_holding_bars' }]
      },
      short: {
        any: []
      }
    },
    positionManagement: {}
  };
}

test('compactEquitySeriesForDate keeps only opening, realized-equity changes, and closing checkpoints for a finished day', () => {
  const engine = new StrategyExecutionEngine();
  const strategy = buildStrategy();
  const state = engine.createRunState({ strategy, runConfig: { orderSize: 1, initialBalance: 1000 } });
  const dayOne = '2026-03-19';
  state.equitySeries = [
    { time: 1, equity: 1000, drawdownPct: 0, date: dayOne },
    { time: 2, equity: 1000, drawdownPct: 0, date: dayOne },
    { time: 3, equity: 1000.005, drawdownPct: 0, date: dayOne },
    { time: 4, equity: 1000.005, drawdownPct: 0, date: dayOne },
    { time: 5, equity: 999.99, drawdownPct: 0.0015, date: dayOne },
    { time: 6, equity: 999.99, drawdownPct: 0.0015, date: dayOne },
    { time: 7, equity: 999.99, drawdownPct: 0.0015, date: '2026-03-20' }
  ];

  engine.compactEquitySeriesForDate({ state, dateLabel: dayOne });
  assert.deepEqual(
    state.equitySeries.map((point) => ({ time: point.time, equity: point.equity, date: point.date })),
    [
      { time: 1, equity: 1000, date: dayOne },
      { time: 3, equity: 1000.005, date: dayOne },
      { time: 5, equity: 999.99, date: dayOne },
      { time: 6, equity: 999.99, date: dayOne },
      { time: 7, equity: 999.99, date: '2026-03-20' }
    ]
  );
});

test('finalizeDay compacts no-trade sessions to opening and closing equity points', () => {
  const engine = new StrategyExecutionEngine({
    ruleEvaluator: {
      evaluateBlock() {
        return false;
      },
      evaluateCondition() {
        return false;
      }
    }
  });
  const strategy = {
    market: {
      allow_long: true,
      allow_short: false
    },
    risk: {
      max_holding_bars: 1
    },
    execution: {
      cooldown_bars_after_exit: 0
    },
    entryRules: {
      long: { any: [] },
      short: { any: [] }
    },
    exitRules: {
      long: { any: [] },
      short: { any: [] }
    },
    positionManagement: {}
  };
  const state = engine.createRunState({ strategy, runConfig: { orderSize: 1, initialBalance: 1000 } });
  const fillModel = engine.createFillModel({ syntheticSpreadBps: 0 });

  const dayOne = '2026-03-20';
  const candles = [
    buildCandle(10, { open: 100, high: 100, low: 100, close: 100 }),
    buildCandle(11, { open: 100, high: 100, low: 100, close: 100 }),
    buildCandle(12, { open: 100, high: 100, low: 100, close: 100 })
  ];

  candles.forEach((candle) => {
    engine.processCandle({ strategy, state, candle, fillModel, currentDateLabel: dayOne });
  });
  engine.finalizeDay({ strategy, state, fillModel, dateLabel: dayOne });

  assert.deepEqual(
    state.equitySeries.map((point) => ({ time: point.time, equity: point.equity, date: point.date })),
    [
      { time: 10, equity: 1000, date: dayOne },
      { time: 12, equity: 1000, date: dayOne }
    ]
  );
});

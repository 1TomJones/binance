const BUILT_IN_STRATEGIES = {
  VWAP_CVD_Live_Trend_01: {
    key: 'VWAP_CVD_Live_Trend_01',
    source: 'built_in',
    fileName: 'VWAP_CVD_Live_Trend_01.json',
    strategy: {
      metadata: {
        name: 'VWAP_CVD_Live_Trend_01',
        version: '1.0.0',
        description: 'VWAP and CVD trend-following market strategy for BTCUSDT session trading.'
      },
      market: {
        symbol: 'BTCUSDT',
        timeframe: '1m',
        allow_long: true,
        allow_short: true
      },
      indicators: {
        session_vwap: true,
        session_cvd: true,
        average_volume_20: true
      },
      execution: {
        evaluation_mode: 'on_candle_close',
        order_type: 'market',
        one_position_at_a_time: true,
        allow_flip: false,
        cooldown_bars_after_exit: 0
      },
      risk: {
        position_size_pct_of_equity: 2,
        stop_loss_pct: 0.35,
        take_profit_pct: 0.7,
        fee_pct_per_side: 0,
        slippage_pct_per_side: 0,
        max_holding_bars: 240
      },
      position_management: {
        enable_break_even: false,
        move_stop_to_break_even_at_profit_pct: 0.4
      },
      entry_rules: {
        long: {
          all: [
            { left: 'close', operator: 'gt', right: 'vwap_session' },
            { left: 'cvd_close', operator: 'gt', right: 'prev_cvd_close' }
          ]
        },
        short: {
          all: [
            { left: 'close', operator: 'lt', right: 'vwap_session' },
            { left: 'cvd_close', operator: 'lt', right: 'prev_cvd_close' }
          ]
        }
      },
      exit_rules: {
        long: {
          any: [
            { left: 'close', operator: 'lt', right: 'vwap_session' },
            { type: 'stop_loss' },
            { type: 'take_profit' },
            { type: 'max_holding_bars' }
          ]
        },
        short: {
          any: [
            { left: 'close', operator: 'gt', right: 'vwap_session' },
            { type: 'stop_loss' },
            { type: 'take_profit' },
            { type: 'max_holding_bars' }
          ]
        }
      },
      backtest_defaults: {
        initial_balance: 10000
      }
    },
    description: 'Live paper trend follower using session VWAP alignment and rising/falling CVD confirmation.',
    entryRules: {
      long: 'Close above session VWAP with rising CVD while flat.',
      short: 'Close below session VWAP with falling CVD while flat.'
    },
    exitRules: {
      long: 'Exit on VWAP loss, stop loss, take profit, max holding bars, or end-of-day flatten.',
      short: 'Exit on VWAP reclaim, stop loss, take profit, max holding bars, or end-of-day flatten.'
    }
  }
};

function buildDescriptor(definition) {
  return {
    key: definition.key,
    id: definition.key,
    name: definition.strategy.metadata.name,
    label: definition.strategy.metadata.name,
    description: definition.description,
    symbol: definition.strategy.market.symbol,
    timeframe: definition.strategy.market.timeframe,
    source: definition.source,
    fileName: definition.fileName,
    entryRules: definition.entryRules,
    exitRules: definition.exitRules,
    summary: {
      version: definition.strategy.metadata.version,
      evaluationMode: definition.strategy.execution.evaluation_mode,
      allows: [
        definition.strategy.market.allow_long ? 'long' : null,
        definition.strategy.market.allow_short ? 'short' : null
      ].filter(Boolean)
    }
  };
}

export function listBuiltInLiveStrategies() {
  return Object.values(BUILT_IN_STRATEGIES).map(buildDescriptor);
}

export function listBuiltInStrategyCatalog() {
  return Object.values(BUILT_IN_STRATEGIES).map(buildDescriptor);
}

export function getBuiltInLiveStrategy(strategyKey) {
  const definition = BUILT_IN_STRATEGIES[strategyKey];
  return definition ? buildDescriptor(definition) : null;
}

export function getBuiltInStrategyDefinition(strategyKey) {
  const definition = BUILT_IN_STRATEGIES[strategyKey];
  if (!definition) return null;
  return {
    ...definition,
    strategy: normalizeStrategy(definition.strategy),
    rawContent: JSON.stringify(definition.strategy, null, 2)
  };
}

function normalizeStrategy(strategy) {
  return {
    metadata: strategy.metadata,
    market: strategy.market,
    indicators: strategy.indicators,
    execution: strategy.execution,
    risk: strategy.risk,
    positionManagement: strategy.position_management,
    entryRules: strategy.entry_rules,
    exitRules: strategy.exit_rules,
    backtestDefaults: strategy.backtest_defaults
  };
}

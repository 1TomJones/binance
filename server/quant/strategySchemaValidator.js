const REQUIRED_SECTIONS = [
  'metadata',
  'market',
  'indicators',
  'execution',
  'risk',
  'position_management',
  'entry_rules',
  'exit_rules',
  'backtest_defaults'
];

const ALLOWED_TIMEFRAMES = new Set(['1m', '5m', '15m', '1h']);
const ALLOWED_OPERATORS = new Set(['gt', 'lt', 'gte', 'lte', 'eq']);
const ALLOWED_WRAPPERS = new Set(['all', 'any']);
const SUPPORTED_VARIABLES = new Set([
  'open', 'high', 'low', 'close', 'volume',
  'prev_close', 'prev_high', 'prev_low', 'prev_volume',
  'vwap_session',
  'cvd_open', 'cvd_high', 'cvd_low', 'cvd_close', 'prev_cvd_close',
  'dom_visible_buy_limits', 'dom_visible_sell_limits',
  'avg_volume_20'
]);

const BUILTIN_EXITS = new Set(['stop_loss', 'take_profit', 'max_holding_bars']);

export class StrategySchemaValidator {
  validate(strategy) {
    const errors = [];
    if (!strategy || typeof strategy !== 'object' || Array.isArray(strategy)) {
      return { valid: false, errors: ['Strategy root must be a JSON object.'] };
    }

    for (const section of REQUIRED_SECTIONS) {
      if (!strategy[section] || typeof strategy[section] !== 'object') {
        errors.push(`Missing required section: ${section}`);
      }
    }

    this.#validateMarket(strategy.market, errors);
    this.#validateExecution(strategy.execution, errors);
    this.#validateRisk(strategy.risk, errors);
    this.#validatePositionManagement(strategy.position_management, errors);
    this.#validateEntryExit('entry_rules', strategy.entry_rules, errors, false);
    this.#validateEntryExit('exit_rules', strategy.exit_rules, errors, true);

    return { valid: errors.length === 0, errors };
  }

  #validateMarket(market, errors) {
    if (!market) return;
    if (typeof market.symbol !== 'string' || !market.symbol) errors.push('market.symbol must be a non-empty string');
    if (!ALLOWED_TIMEFRAMES.has(market.timeframe)) errors.push(`market.timeframe must be one of ${[...ALLOWED_TIMEFRAMES].join(', ')}`);
    if (typeof market.allow_long !== 'boolean') errors.push('market.allow_long must be boolean');
    if (typeof market.allow_short !== 'boolean') errors.push('market.allow_short must be boolean');
  }

  #validateExecution(execution, errors) {
    if (!execution) return;
    if (execution.evaluation_mode !== 'on_candle_close') errors.push('execution.evaluation_mode must be on_candle_close');
    if (execution.order_type !== 'market') errors.push('execution.order_type must be market for v1');
    for (const field of ['one_position_at_a_time', 'allow_flip']) {
      if (typeof execution[field] !== 'boolean') errors.push(`execution.${field} must be boolean`);
    }
    if (!Number.isInteger(execution.cooldown_bars_after_exit) || execution.cooldown_bars_after_exit < 0) {
      errors.push('execution.cooldown_bars_after_exit must be a non-negative integer');
    }
  }

  #validateRisk(risk, errors) {
    if (!risk) return;
    this.#pct(risk.position_size_pct_of_equity, 'risk.position_size_pct_of_equity', errors, 0.01, 100);
    this.#pct(risk.stop_loss_pct, 'risk.stop_loss_pct', errors, 0.001, 100);
    this.#pct(risk.take_profit_pct, 'risk.take_profit_pct', errors, 0.001, 200);
    this.#pct(risk.fee_pct_per_side, 'risk.fee_pct_per_side', errors, 0, 10);
    this.#pct(risk.slippage_pct_per_side, 'risk.slippage_pct_per_side', errors, 0, 10);
    if (!Number.isInteger(risk.max_holding_bars) || risk.max_holding_bars < 1) errors.push('risk.max_holding_bars must be an integer >= 1');
  }

  #validatePositionManagement(config, errors) {
    if (!config) return;
    if (typeof config.enable_break_even !== 'boolean') errors.push('position_management.enable_break_even must be boolean');
    this.#pct(config.move_stop_to_break_even_at_profit_pct, 'position_management.move_stop_to_break_even_at_profit_pct', errors, 0, 100);
  }

  #validateEntryExit(path, rules, errors, isExit) {
    if (!rules) return;
    for (const side of ['long', 'short']) {
      const sideRules = rules[side];
      if (!sideRules || typeof sideRules !== 'object') {
        errors.push(`${path}.${side} must exist`);
        continue;
      }

      const wrappers = Object.keys(sideRules);
      if (wrappers.length !== 1 || !ALLOWED_WRAPPERS.has(wrappers[0])) {
        errors.push(`${path}.${side} must contain exactly one wrapper: all or any`);
        continue;
      }

      const conditions = sideRules[wrappers[0]];
      if (!Array.isArray(conditions) || conditions.length === 0) {
        errors.push(`${path}.${side}.${wrappers[0]} must be a non-empty array`);
        continue;
      }

      conditions.forEach((condition, index) => {
        const label = `${path}.${side}.${wrappers[0]}[${index}]`;
        if (!condition || typeof condition !== 'object') {
          errors.push(`${label} must be an object`);
          return;
        }

        if (isExit && condition.type) {
          if (!BUILTIN_EXITS.has(condition.type)) errors.push(`${label}.type must be one of ${[...BUILTIN_EXITS].join(', ')}`);
          return;
        }

        if (!SUPPORTED_VARIABLES.has(condition.left)) errors.push(`${label}.left references unsupported variable`);
        if (!ALLOWED_OPERATORS.has(condition.operator)) errors.push(`${label}.operator must be one of ${[...ALLOWED_OPERATORS].join(', ')}`);
        if (!SUPPORTED_VARIABLES.has(condition.right)) errors.push(`${label}.right references unsupported variable`);
      });
    }
  }

  #pct(value, field, errors, min, max) {
    if (typeof value !== 'number' || Number.isNaN(value)) errors.push(`${field} must be a number`);
    else if (value < min || value > max) errors.push(`${field} must be between ${min} and ${max}`);
  }
}

export const STRATEGY_V1_CONSTANTS = {
  ALLOWED_OPERATORS: [...ALLOWED_OPERATORS],
  SUPPORTED_VARIABLES: [...SUPPORTED_VARIABLES],
  ALLOWED_TIMEFRAMES: [...ALLOWED_TIMEFRAMES]
};

import { StrategySchemaValidator } from './strategySchemaValidator.js';

export class StrategyParser {
  constructor({ validator = new StrategySchemaValidator() } = {}) {
    this.validator = validator;
  }

  parse(content) {
    let payload;
    try {
      payload = JSON.parse(content);
    } catch {
      return { valid: false, errors: ['Strategy file must be valid JSON.'], strategy: null, summary: null };
    }

    const validation = this.validator.validate(payload);
    if (!validation.valid) {
      return { valid: false, errors: validation.errors, strategy: null, summary: null };
    }

    const strategy = {
      metadata: payload.metadata,
      market: payload.market,
      indicators: payload.indicators,
      execution: payload.execution,
      risk: payload.risk,
      positionManagement: payload.position_management,
      entryRules: payload.entry_rules,
      exitRules: payload.exit_rules,
      backtestDefaults: payload.backtest_defaults
    };

    return {
      valid: true,
      errors: [],
      strategy,
      summary: {
        name: strategy.metadata.name,
        version: strategy.metadata.version,
        symbol: strategy.market.symbol,
        timeframe: strategy.market.timeframe,
        evaluationMode: strategy.execution.evaluation_mode,
        allows: [strategy.market.allow_long ? 'long' : null, strategy.market.allow_short ? 'short' : null].filter(Boolean)
      }
    };
  }
}

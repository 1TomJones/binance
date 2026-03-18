export class LiveStrategyRunner {
  constructor() {
    this.state = {
      status: 'idle',
      strategyLoaded: false,
      symbol: 'BTCUSDT',
      timeframe: '1m',
      currentEquity: null,
      unrealizedPnl: null,
      realizedPnl: null,
      returnPct: null,
      openPositions: 0,
      tradeCount: 0,
      winRate: null,
      drawdown: null,
      lastSignal: null,
      lastAction: null,
      mode: 'paper_live'
    };
  }

  getSnapshot() {
    return this.state;
  }

  update(patch) {
    this.state = { ...this.state, ...patch };
    return this.state;
  }
}

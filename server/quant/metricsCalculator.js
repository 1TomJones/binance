export class MetricsCalculator {
  calculate({ initialBalance, equitySeries, trades, openPosition, lastPrice, cumulativeRealizedSeries = [] }) {
    const closedTrades = trades.filter((trade) => trade.status === 'closed');
    const ending = equitySeries.at(-1)?.equity ?? initialBalance;
    const realizedPnl = closedTrades.reduce((acc, trade) => acc + trade.realizedPnl, 0);
    const unrealizedPnl = openPosition ? this.#calcUnrealized(openPosition, lastPrice) : 0;
    const wins = closedTrades.filter((trade) => trade.realizedPnl > 0);
    const losses = closedTrades.filter((trade) => trade.realizedPnl < 0);
    const breakeven = closedTrades.filter((trade) => trade.realizedPnl === 0);
    const grossProfit = wins.reduce((acc, trade) => acc + trade.realizedPnl, 0);
    const grossLoss = Math.abs(losses.reduce((acc, trade) => acc + trade.realizedPnl, 0));
    const returns = this.#seriesReturns(equitySeries);
    const tradePnls = closedTrades.map((x) => x.realizedPnl);
    const durations = closedTrades.map((x) => x.durationMinutes || 0);

    return {
      netPnl: round(ending + unrealizedPnl - initialBalance),
      returnPct: round(((ending + unrealizedPnl - initialBalance) / Math.max(initialBalance, 1e-9)) * 100),
      winRate: round((wins.length / Math.max(closedTrades.length, 1)) * 100),
      totalTrades: closedTrades.length,
      wins: wins.length,
      losses: losses.length,
      breakeven: breakeven.length,
      openTrades: openPosition ? 1 : 0,
      averageTradePnl: round(avg(tradePnls)),
      bestTrade: round(Math.max(...tradePnls, 0)),
      worstTrade: round(Math.min(...tradePnls, 0)),
      profitFactor: round(grossLoss ? grossProfit / grossLoss : grossProfit > 0 ? 999 : 0),
      maxDrawdown: round(Math.max(...equitySeries.map((p) => p.drawdownPct || 0), 0)),
      sharpeRatio: round(this.#sharpe(returns)),
      expectancy: round(avg(tradePnls)),
      averageHoldingBars: round(avg(closedTrades.map((x) => x.holdingBars))),
      realizedPnl: round(realizedPnl),
      cumulativeRealizedPnl: round(realizedPnl),
      unrealizedPnl: round(unrealizedPnl),
      currentEquity: round(ending + unrealizedPnl),
      averageTradeDurationMinutes: round(avg(durations)),
      medianTradeDurationMinutes: round(median(durations)),
      cumulativePnlPoints: cumulativeRealizedSeries.length
    };
  }

  buildAnalyses({ trades }) {
    const closedTrades = trades.filter((trade) => trade.status === 'closed');
    const outcome = {
      winning: closedTrades.filter((trade) => trade.realizedPnl > 0).length,
      losing: closedTrades.filter((trade) => trade.realizedPnl < 0).length,
      breakeven: closedTrades.filter((trade) => trade.realizedPnl === 0).length
    };

    const hours = Array.from({ length: 24 }, (_, hour) => {
      const hourTrades = closedTrades.filter((trade) => new Date(trade.entryTime).getUTCHours() === hour);
      const wins = hourTrades.filter((trade) => trade.realizedPnl > 0).length;
      return {
        hour,
        label: `${String(hour).padStart(2, '0')}:00–${String((hour + 1) % 24).padStart(2, '0')}:00`,
        tradeCount: hourTrades.length,
        winRate: round((wins / Math.max(hourTrades.length, 1)) * 100),
        averagePnl: round(avg(hourTrades.map((trade) => trade.realizedPnl)))
      };
    });

    const durationBuckets = [
      { key: '<5m', min: 0, max: 5 },
      { key: '5-15m', min: 5, max: 15 },
      { key: '15-30m', min: 15, max: 30 },
      { key: '30-60m', min: 30, max: 60 },
      { key: '60m+', min: 60, max: Infinity }
    ].map((bucket) => ({
      ...bucket,
      count: closedTrades.filter((trade) => {
        const minutes = Number(trade.durationMinutes || 0);
        return minutes >= bucket.min && minutes < bucket.max;
      }).length
    }));

    const exitReasons = ['take_profit', 'stop_loss', 'signal_exit', 'end_of_day_exit', 'max_holding_bars']
      .map((reason) => ({
        reason,
        count: closedTrades.filter((trade) => trade.exitReason === reason).length
      }))
      .filter((item) => item.count > 0);

    return {
      outcome,
      timeOfDay: hours,
      durationBuckets,
      exitReasons
    };
  }

  #calcUnrealized(position, markPrice) {
    if (!markPrice) return 0;
    if (position.side === 'long') return (markPrice - position.entryPrice) * position.quantity;
    return (position.entryPrice - markPrice) * position.quantity;
  }

  #seriesReturns(series) {
    const values = [];
    for (let i = 1; i < series.length; i += 1) {
      const prev = series[i - 1].equity;
      const cur = series[i].equity;
      values.push(prev ? (cur - prev) / prev : 0);
    }
    return values;
  }

  #sharpe(returns) {
    if (!returns.length) return 0;
    const mean = avg(returns);
    const variance = avg(returns.map((ret) => (ret - mean) ** 2));
    const std = Math.sqrt(variance);
    if (!std) return 0;
    return (mean / std) * Math.sqrt(returns.length);
  }
}

function avg(values) {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function round(value) {
  return Number((value || 0).toFixed(4));
}

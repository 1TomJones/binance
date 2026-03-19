import test from 'node:test';
import assert from 'node:assert/strict';

import { deriveBacktestProgress } from '../client/src/utils/backtestProgress.js';

test('deriveBacktestProgress surfaces the exact job error message for failed backtests', () => {
  const progress = deriveBacktestProgress({
    status: 'failed',
    progress_pct: 42,
    current_marker: 'Failed',
    progress_json: JSON.stringify({
      phase: 'failed',
      hydration: {
        source: 'binance-rest',
        status: 'retrying',
        rowsIngested: 1000,
        pagesIngested: 1,
        checkpointTimeMs: 1710028801000,
        lastAggTradeId: 123,
        retry: {
          attempt: 2,
          retryInMs: 1000,
          message: 'server busy'
        },
        percent: 12
      },
      replay: {
        status: 'pending',
        replayedTrades: 0,
        totalTrades: 0,
        percent: 0
      }
    }),
    current_date: '2026-03-15',
    current_day: 2,
    total_days: 3,
    closed_trade_count: 7,
    elapsed_ms: 1234,
    error_message: 'Unable to hydrate historical trades for BTCUSDT on 2026-03-15. Binance responded 500.'
  });

  assert.equal(progress.status, 'Failed');
  assert.equal(progress.marker, 'Failed');
  assert.equal(progress.errorMessage, 'Unable to hydrate historical trades for BTCUSDT on 2026-03-15. Binance responded 500.');
  assert.equal(progress.currentDayLabel, 'Day 2 / 3');
  assert.equal(progress.phase, 'failed');
  assert.equal(progress.hydration?.retry?.attempt, 2);
});

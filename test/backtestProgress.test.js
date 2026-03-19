import test from 'node:test';
import assert from 'node:assert/strict';

import { deriveBacktestProgress } from '../client/src/utils/backtestProgress.js';

test('deriveBacktestProgress surfaces the exact job error message for failed backtests', () => {
  const progress = deriveBacktestProgress({
    status: 'failed',
    progress_pct: 42,
    current_marker: 'Failed',
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
});

import test from 'node:test';
import assert from 'node:assert/strict';

import { deriveBacktestProgress } from '../client/src/utils/backtestProgress.js';
import { buildDefaultBacktestDateRange, rangeIncludesCurrentUtcDay, shouldPollLiveWorkspace } from '../client/src/utils/quantWorkspaceMode.js';

test('deriveBacktestProgress surfaces the exact job error message for failed backtests', () => {
  const progress = deriveBacktestProgress({
    status: 'failed',
    progress_pct: 42,
    current_marker: 'Failed',
    progress_json: JSON.stringify({
      phase: 'failed',
      coverage: {
        totalDays: 3,
        readyDays: 1,
        hydratableDays: 2,
        waitingOnCoverage: true,
        hydratingDay: '2026-03-15',
        includeCurrentDay: false,
        requestedCurrentUtcDay: false,
        currentUtcDaySlowPath: false,
        classifications: []
      },
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
  assert.equal(progress.coverage?.hydratableDays, 2);
});

test('deriveBacktestProgress promotes the simpler top-level backtest phases', () => {
  const progress = deriveBacktestProgress({
    status: 'running',
    progress_pct: 15,
    current_marker: 'Preparing historical coverage',
    progress_json: JSON.stringify({
      phase: 'hydrating',
      coverage: {
        totalDays: 4,
        readyDays: 1,
        hydratableDays: 3,
        waitingOnCoverage: true,
        hydratingDay: '2026-03-18',
        includeCurrentDay: true,
        requestedCurrentUtcDay: true,
        currentUtcDaySlowPath: true,
        classifications: []
      },
      hydration: null,
      replay: null
    })
  });

  assert.equal(progress.primaryLabel, 'Preparing historical coverage');
  assert.equal(progress.coverage?.currentUtcDaySlowPath, true);
});

test('backtest mode does not poll live workspace metrics while live mode still does', () => {
  assert.equal(shouldPollLiveWorkspace('backtest'), false);
  assert.equal(shouldPollLiveWorkspace('live'), true);
});

test('default backtest range prefers completed UTC days only and excludes today', () => {
  const defaults = buildDefaultBacktestDateRange(new Date('2026-03-19T12:00:00.000Z'));
  assert.equal(defaults.defaultEndDate, '2026-03-18');
  assert.equal(rangeIncludesCurrentUtcDay(defaults.defaultStartDate, defaults.defaultEndDate, new Date('2026-03-19T12:00:00.000Z')), false);
  assert.equal(rangeIncludesCurrentUtcDay('2026-03-18', '2026-03-19', new Date('2026-03-19T12:00:00.000Z')), true);
});


test('deriveBacktestProgress computes an ETA while a backtest is actively running', () => {
  const progress = deriveBacktestProgress({
    status: 'running',
    progress_pct: 40,
    elapsed_ms: 20_000,
    current_marker: 'Replaying historical sessions',
    progress_json: JSON.stringify({
      phase: 'replaying',
      coverage: null,
      hydration: null,
      replay: {
        day: '2026-03-18',
        status: 'running',
        replayedTrades: 400,
        totalTrades: 1000,
        percent: 40
      }
    })
  });

  assert.equal(progress.progressLabel, '400 / 1000 trades · 40%');
  assert.equal(progress.secondaryLabel, '2026-03-18 · Running');
  assert.equal(progress.etaMs, 30000);
});

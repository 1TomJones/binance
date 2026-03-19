import test from 'node:test';
import assert from 'node:assert/strict';
import {
  deriveLastClosedUtcDateFromCandleOpenTime,
  deriveSuggestedBacktestRange,
  normalizeBacktestDateRange,
  shiftUtcDate
} from '../server/marketTime.js';

test('deriveLastClosedUtcDateFromCandleOpenTime rolls intraday candles back to the prior UTC day', () => {
  const latestOpenMs = Date.parse('2026-03-19T13:42:00.000Z');
  assert.equal(deriveLastClosedUtcDateFromCandleOpenTime(latestOpenMs), '2026-03-18');
});

test('deriveLastClosedUtcDateFromCandleOpenTime keeps a fully closed 23:59 candle on the same UTC day', () => {
  const latestOpenMs = Date.parse('2026-03-19T23:59:00.000Z');
  assert.equal(deriveLastClosedUtcDateFromCandleOpenTime(latestOpenMs), '2026-03-19');
});

test('deriveSuggestedBacktestRange returns a recent five-day window ending at the latest closed UTC day', () => {
  const latestOpenMs = Date.parse('2026-03-19T13:42:00.000Z');
  assert.deepEqual(deriveSuggestedBacktestRange({ latestCandleOpenMs: latestOpenMs }), {
    startDate: '2026-03-14',
    endDate: '2026-03-18',
    speed: 'fast'
  });
});

test('normalizeBacktestDateRange clamps future ranges back to available history', () => {
  const latestOpenMs = Date.parse('2026-03-19T13:42:00.000Z');
  const normalized = normalizeBacktestDateRange({
    startDate: '2026-03-25',
    endDate: '2026-03-30',
    latestCandleOpenMs: latestOpenMs
  });

  assert.equal(normalized.startDate, '2026-03-14');
  assert.equal(normalized.endDate, '2026-03-18');
  assert.equal(normalized.adjusted, true);
  assert.equal(normalized.reason, 'entire_range_shifted_to_recent_history');
});

test('shiftUtcDate preserves YYYY-MM-DD formatting across day offsets', () => {
  assert.equal(shiftUtcDate('2026-03-19', -4), '2026-03-15');
});

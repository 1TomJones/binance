const DAY_MS = 24 * 60 * 60 * 1000;

export function formatUtcDate(tsMs) {
  return new Date(tsMs).toISOString().slice(0, 10);
}

export function shiftUtcDate(dateString, deltaDays) {
  const baseMs = Date.parse(`${dateString}T00:00:00.000Z`);
  if (!Number.isFinite(baseMs)) return dateString;
  return formatUtcDate(baseMs + (deltaDays * DAY_MS));
}

export function deriveLastClosedUtcDateFromCandleOpenTime(latestCandleOpenMs) {
  if (!Number.isFinite(latestCandleOpenMs)) return null;

  const latest = new Date(latestCandleOpenMs);
  const isSessionCloseCandle = latest.getUTCHours() === 23 && latest.getUTCMinutes() === 59;
  return formatUtcDate(isSessionCloseCandle ? latestCandleOpenMs : latestCandleOpenMs - DAY_MS);
}

export function deriveSuggestedBacktestRange({ latestCandleOpenMs, lookbackDays = 5 } = {}) {
  const endDate = deriveLastClosedUtcDateFromCandleOpenTime(latestCandleOpenMs);
  if (!endDate) return null;

  return {
    startDate: shiftUtcDate(endDate, -(Math.max(lookbackDays, 1) - 1)),
    endDate,
    speed: 'fast'
  };
}

export function normalizeBacktestDateRange({ startDate, endDate, latestCandleOpenMs, lookbackDays = 5 } = {}) {
  const suggested = deriveSuggestedBacktestRange({ latestCandleOpenMs, lookbackDays });
  if (!suggested) {
    return {
      startDate,
      endDate,
      suggested,
      adjusted: false,
      reason: null
    };
  }

  let normalizedStartDate = startDate || suggested.startDate;
  let normalizedEndDate = endDate || suggested.endDate;
  let adjusted = false;
  let reason = null;

  if (normalizedEndDate > suggested.endDate) {
    normalizedEndDate = suggested.endDate;
    adjusted = true;
    reason = 'end_date_clamped_to_latest_closed_session';
  }

  if (normalizedStartDate > suggested.endDate) {
    normalizedStartDate = suggested.startDate;
    normalizedEndDate = suggested.endDate;
    adjusted = true;
    reason = 'entire_range_shifted_to_recent_history';
  }

  if (normalizedStartDate > normalizedEndDate) {
    normalizedStartDate = shiftUtcDate(normalizedEndDate, -(Math.max(lookbackDays, 1) - 1));
    adjusted = true;
    reason = 'start_date_shifted_before_end_date';
  }

  return {
    startDate: normalizedStartDate,
    endDate: normalizedEndDate,
    suggested,
    adjusted,
    reason
  };
}

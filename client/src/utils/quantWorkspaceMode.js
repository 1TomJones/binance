export function shouldPollLiveWorkspace(mode) {
  return mode === 'live';
}

export function buildDefaultBacktestDateRange(today = new Date()) {
  const end = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()) - 86400000);
  const start = new Date(end.getTime() - 6 * 86400000);
  return {
    defaultStartDate: toDateInput(start),
    defaultEndDate: toDateInput(end)
  };
}

export function rangeIncludesCurrentUtcDay(startDate, endDate, now = new Date()) {
  if (!startDate || !endDate) return false;
  const currentUtcDate = toDateInput(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())));
  return startDate <= currentUtcDate && endDate >= currentUtcDate;
}

function toDateInput(date) {
  return new Date(date).toISOString().slice(0, 10);
}

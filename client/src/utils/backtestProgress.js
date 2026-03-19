export function deriveBacktestProgress(job) {
  const latest = job?.status ? job : null;
  const currentDay = Number(latest?.current_day || 0);
  const totalDays = Number(latest?.total_days || 0);
  const errorMessage = latest?.error_message || '';
  const fallbackMarker = latest?.current_marker || 'Waiting to start';

  return {
    status: humanizeStatus(latest?.status || 'ready'),
    currentDate: latest?.current_date || parseCurrentDate(latest?.current_marker) || '—',
    currentDayLabel: currentDay && totalDays ? `Day ${currentDay} / ${totalDays}` : '—',
    percent: latest?.progress_pct || 0,
    elapsedMs: latest?.elapsed_ms || 0,
    totalTrades: latest?.closed_trade_count || parseTradeCount(latest?.current_marker),
    marker: latest?.status === 'failed' && errorMessage ? 'Failed' : fallbackMarker,
    errorMessage
  };
}

function humanizeStatus(value) {
  const map = {
    ready: 'Ready',
    queued: 'Queued',
    running: 'Running',
    completed: 'Completed',
    failed: 'Failed',
    cancelled: 'Cancelled'
  };
  return map[value] || value || 'Ready';
}

function parseCurrentDate(marker) {
  return marker?.match(/\d{4}-\d{2}-\d{2}/)?.[0] || null;
}

function parseTradeCount(marker) {
  return Number(marker?.match(/(\d+) trades/)?.[1] || 0);
}

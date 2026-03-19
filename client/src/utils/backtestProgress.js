export function deriveBacktestProgress(job) {
  const latest = job?.status ? job : null;
  const currentDay = Number(latest?.current_day || 0);
  const totalDays = Number(latest?.total_days || 0);
  const errorMessage = latest?.error_message || '';
  const fallbackMarker = latest?.current_marker || 'Waiting to start';
  const progressDetails = parseProgressJson(latest?.progress_json);

  return {
    status: humanizeStatus(latest?.status || 'ready'),
    currentDate: latest?.current_date || parseCurrentDate(latest?.current_marker) || '—',
    currentDayLabel: currentDay && totalDays ? `Day ${currentDay} / ${totalDays}` : '—',
    percent: latest?.progress_pct || 0,
    elapsedMs: latest?.elapsed_ms || 0,
    totalTrades: latest?.closed_trade_count || parseTradeCount(latest?.current_marker),
    marker: latest?.status === 'failed' && errorMessage ? 'Failed' : fallbackMarker,
    errorMessage,
    phase: progressDetails?.phase || null,
    coverage: progressDetails?.coverage || null,
    hydration: progressDetails?.hydration || null,
    replay: progressDetails?.replay || null,
    primaryLabel: derivePrimaryLabel(progressDetails?.phase, progressDetails?.coverage)
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

function parseProgressJson(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function derivePrimaryLabel(phase, coverage) {
  if (phase === 'planning' || phase === 'hydrating') return 'Preparing historical coverage';
  if (phase === 'replaying') return 'Replaying historical sessions';
  if (phase === 'finalizing') return 'Finalizing results';
  if (phase === 'completed') return 'Completed';
  if (phase === 'failed') return 'Failed';
  if (phase === 'cancelled') return 'Cancelled';
  if (coverage?.waitingOnCoverage) return 'Preparing historical coverage';
  return 'Waiting to start';
}

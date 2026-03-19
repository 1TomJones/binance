export function deriveBacktestProgress(job) {
  const latest = job?.status ? job : null;
  const currentDay = Number(latest?.current_day || 0);
  const totalDays = Number(latest?.total_days || 0);
  const errorMessage = latest?.error_message || '';
  const fallbackMarker = latest?.current_marker || 'Waiting to start';
  const progressDetails = parseProgressJson(latest?.progress_json);
  const rawPercent = Number(latest?.progress_pct);
  const percent = Number.isFinite(rawPercent)
    ? clamp(rawPercent, 0, latest?.status === 'completed' ? 100 : 99)
    : 0;
  const elapsedMs = Number(latest?.elapsed_ms || 0);
  const etaMs = deriveEtaMs({ percent, elapsedMs, status: latest?.status });
  const coverage = progressDetails?.coverage || null;
  const hydration = progressDetails?.hydration || null;
  const replay = progressDetails?.replay || null;
  const phase = progressDetails?.phase || null;
  const primaryLabel = derivePrimaryLabel(phase, coverage);
  const secondaryLabel = deriveSecondaryLabel({ phase, hydration, replay, fallbackMarker });

  return {
    status: humanizeStatus(latest?.status || 'ready'),
    currentDate: latest?.current_date || parseCurrentDate(latest?.current_marker) || '—',
    currentDayLabel: currentDay && totalDays ? `Day ${currentDay} / ${totalDays}` : '—',
    percent,
    elapsedMs,
    etaMs,
    totalTrades: latest?.closed_trade_count || parseTradeCount(latest?.current_marker),
    marker: latest?.status === 'failed' && errorMessage ? 'Failed' : fallbackMarker,
    errorMessage,
    phase,
    coverage,
    hydration,
    replay,
    primaryLabel,
    secondaryLabel,
    progressLabel: buildProgressLabel({ percent, phase, replay })
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

function deriveSecondaryLabel({ phase, hydration, replay, fallbackMarker }) {
  if (phase === 'hydrating' && hydration?.source) {
    return `${humanizeHydrationSource(hydration.source)} · ${humanizeHydrationStatus(hydration.status)}`;
  }
  if (phase === 'replaying' && replay?.day) {
    return `${replay.day} · ${humanizeHydrationStatus(replay.status)}`;
  }
  return fallbackMarker || 'Waiting to start';
}

function buildProgressLabel({ percent, phase, replay }) {
  if (phase === 'replaying' && replay?.replayedTrades && replay?.totalTrades) {
    return `${formatCompactNumber(replay.replayedTrades)} / ${formatCompactNumber(replay.totalTrades)} trades · ${formatCompactNumber(percent)}%`;
  }
  return `${formatCompactNumber(percent)}%`;
}

function deriveEtaMs({ percent, elapsedMs, status }) {
  if (status !== 'running') return null;
  if (!Number.isFinite(percent) || percent <= 0 || percent >= 100) return null;
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) return null;
  return Math.max(Math.round((elapsedMs / percent) * (100 - percent)), 0);
}

function humanizeHydrationSource(value) {
  const map = {
    'bulk-file': 'Binance bulk ZIP',
    'binance-rest': 'Binance REST',
    'binance-rest-fallback': 'REST fallback',
    'current-day-tail': 'Current-day tail',
    'sqlite-reconciled': 'Local SQLite'
  };
  return map[value] || value || '—';
}

function humanizeHydrationStatus(value) {
  const map = {
    pending: 'Pending',
    running: 'Running',
    complete: 'Complete',
    completed: 'Completed',
    retrying: 'Retrying',
    fallback: 'Fallback',
    hydration: 'Hydration',
    planning: 'Planning',
    hydrating: 'Hydrating',
    replaying: 'Replaying',
    finalizing: 'Finalizing',
    failed: 'Failed',
    cancelled: 'Cancelled'
  };
  return map[value] || value || '—';
}

function clamp(value, min, max) {
  return Math.min(Math.max(Number(value) || 0, min), max);
}

function formatCompactNumber(value) {
  return Number(Number(value || 0).toFixed(value >= 100 ? 0 : 1)).toString();
}

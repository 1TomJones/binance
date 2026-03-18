async function request(url, options = {}) {
  const response = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });

  const contentType = response.headers.get('content-type') || '';
  const isJson = contentType.includes('application/json');
  const payload = isJson ? await response.json() : await response.text();

  if (!isJson) {
    const snippet = String(payload).replace(/\s+/g, ' ').trim().slice(0, 120);
    throw new Error(
      `Expected JSON from ${url}, but received ${contentType || 'an unknown response type'}${snippet ? `: ${snippet}` : ''}`
    );
  }

  const data = payload;
  if (!response.ok) {
    throw new Error(data.error || data.validation?.errors?.join(', ') || 'Request failed');
  }
  return data;
}

export const quantApi = {
  getStrategyCatalog: () => request('/api/quant/strategies/catalog'),
  uploadStrategy: (payload) => request('/api/quant/strategy/upload', { method: 'POST', body: JSON.stringify(payload) }),
  getLiveStrategies: () => request('/api/quant/live/strategies'),
  getLiveWorkspace: () => request('/api/quant/live-metrics'),
  startLivePaper: (payload) => request('/api/quant/live/start', { method: 'POST', body: JSON.stringify(payload) }),
  stopLivePaper: () => request('/api/quant/live/stop', { method: 'POST', body: '{}' }),
  startBacktest: (payload) => request('/api/quant/backtests', { method: 'POST', body: JSON.stringify(payload) }),
  cancelBacktest: (jobId) => request(`/api/quant/backtests/${jobId}/cancel`, { method: 'POST', body: '{}' }),
  getBacktestJob: (jobId) => request(`/api/quant/backtests/${jobId}`)
};

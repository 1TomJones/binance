import test from 'node:test';
import assert from 'node:assert/strict';
import { BinanceStreamService } from '../server/binanceStream.js';

function createJsonResponse(payload, { ok = true, status = 200 } = {}) {
  return {
    ok,
    status,
    async json() {
      return payload;
    },
    async text() {
      return JSON.stringify(payload);
    }
  };
}

test('loadDepthSnapshot falls back to later Binance REST hosts and preserves depth sync behavior', async () => {
  const originalFetch = global.fetch;
  const urls = [];

  global.fetch = async (url) => {
    urls.push(url);
    if (urls.length === 1) {
      return createJsonResponse({ code: -1003, msg: 'rate limited' }, { ok: false, status: 418 });
    }

    return createJsonResponse({
      lastUpdateId: 123,
      bids: [['100', '2.5'], ['99', '1']],
      asks: [['101', '3'], ['102', '4']]
    });
  };

  try {
    const service = new BinanceStreamService({ symbol: 'btcusdt' });
    const loaded = await service.loadDepthSnapshot();

    assert.equal(loaded, true);
    assert.equal(service.depthReady, true);
    assert.equal(service.lastUpdateId, 123);
    assert.equal(service.bids.get(100), 2.5);
    assert.equal(service.asks.get(101), 3);
    assert.equal(urls.length, 2);
    assert.match(urls[0], /https:\/\/api\.binance\.com\/api\/v3\/depth/);
    assert.match(urls[1], /depth\?symbol=BTCUSDT&limit=1000$/);
  } finally {
    global.fetch = originalFetch;
  }
});

test('loadDepthSnapshot degrades gracefully when Binance rejects snapshot requests', async () => {
  const originalFetch = global.fetch;
  const originalDateNow = Date.now;
  const scheduledRetries = [];

  Date.now = () => 1_000_000_000_000;
  global.fetch = async () => ({
    ok: false,
    status: 418,
    async json() {
      throw new Error('json should not be called for 418 responses');
    },
    async text() {
      return '{"code":-1003,"msg":"Way too much request weight used; IP banned until 1000000006000. Please use WebSocket Streams for live updates to avoid bans."}';
    }
  });

  try {
    const service = new BinanceStreamService({ symbol: 'btcusdt' });
    service.scheduleDepthSnapshotRetry = (delayMs) => {
      scheduledRetries.push(delayMs);
    };

    await assert.doesNotReject(async () => service.loadDepthSnapshot());

    assert.equal(service.depthReady, false);
    assert.equal(service.lastUpdateId, null);
    assert.equal(scheduledRetries.length, 1);
    assert.equal(scheduledRetries[0], 6_000);
  } finally {
    global.fetch = originalFetch;
    Date.now = originalDateNow;
  }
});

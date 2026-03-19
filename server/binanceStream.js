import WebSocket from 'ws';

const WS_BASE = 'wss://stream.binance.com:9443/ws';
const REST_BASES = [
  process.env.BINANCE_REST_URL,
  'https://api.binance.com/api/v3',
  'https://api1.binance.com/api/v3',
  'https://data-api.binance.vision/api/v3'
].filter(Boolean);
const DEPTH_SNAPSHOT_RETRY_MS = 5_000;

function buildDepthPayload(symbol, bids, asks, ts) {
  const topBids = [...bids.entries()]
    .sort((a, b) => b[0] - a[0])
    .slice(0, 100)
    .map(([price, quantity]) => ({ price, quantity }));

  const topAsks = [...asks.entries()]
    .sort((a, b) => a[0] - b[0])
    .slice(0, 100)
    .map(([price, quantity]) => ({ price, quantity }));

  let bidCum = 0;
  const bidsWithCum = topBids.map((level) => {
    bidCum += level.quantity;
    return { ...level, cumulative: bidCum };
  });

  let askCum = 0;
  const asksWithCum = topAsks.map((level) => {
    askCum += level.quantity;
    return { ...level, cumulative: askCum };
  });

  const bestBid = bidsWithCum[0] || null;
  const bestAsk = asksWithCum[0] || null;

  return {
    symbol,
    bids: bidsWithCum,
    asks: asksWithCum,
    bestBid,
    bestAsk,
    spread: bestBid && bestAsk ? bestAsk.price - bestBid.price : null,
    ts
  };
}

function parseRetryAfterMs(responseBody = '') {
  const bannedUntilMatch = String(responseBody).match(/until\s+(\d{13})/i);
  if (bannedUntilMatch) {
    return Math.max(DEPTH_SNAPSHOT_RETRY_MS, Number(bannedUntilMatch[1]) - Date.now());
  }

  return DEPTH_SNAPSHOT_RETRY_MS;
}

export class BinanceStreamService {
  constructor({ symbol = 'btcusdt', onTrade, onBookTicker, onDepth, onCandle, onCandleBootstrap, onTradeConnected }) {
    this.symbol = symbol.toLowerCase();
    this.onTrade = onTrade;
    this.onBookTicker = onBookTicker;
    this.onDepth = onDepth;
    this.onCandle = onCandle;
    this.onCandleBootstrap = onCandleBootstrap;
    this.onTradeConnected = onTradeConnected;

    this.tradeSocket = null;
    this.bookSocket = null;
    this.depthSocket = null;
    this.klineSocket = null;

    this.tradeReconnectTimer = null;
    this.bookReconnectTimer = null;
    this.depthReconnectTimer = null;
    this.klineReconnectTimer = null;
    this.depthSnapshotRetryTimer = null;

    this.bids = new Map();
    this.asks = new Map();
    this.lastUpdateId = null;
    this.depthReady = false;
    this.depthBuffer = [];
    this.resyncInFlight = false;
    this.lastDepthEmit = 0;
    this.depthEmitTimer = null;
  }

  async start() {
    await this.bootstrapCandles();
    this.connectTrade();
    this.connectBookTicker();
    this.connectDepth();
    this.connectKline();
  }

  stop() {
    clearTimeout(this.tradeReconnectTimer);
    clearTimeout(this.bookReconnectTimer);
    clearTimeout(this.depthReconnectTimer);
    clearTimeout(this.klineReconnectTimer);
    clearTimeout(this.depthSnapshotRetryTimer);
    clearTimeout(this.depthEmitTimer);
    this.tradeSocket?.close();
    this.bookSocket?.close();
    this.depthSocket?.close();
    this.klineSocket?.close();
  }

  async bootstrapCandles() {
    try {
      const url = `${REST_BASES[0]}/klines?symbol=${this.symbol.toUpperCase()}&interval=1m&limit=400`;
      const response = await fetch(url);
      if (!response.ok) return;
      const data = await response.json();
      const candles = data.map((row) => ({
        time: Math.floor(row[0] / 1000),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5])
      }));
      this.onCandleBootstrap?.(candles);
    } catch (_e) {
      // ignore bootstrap failure and continue with streaming
    }
  }

  connectTrade() {
    const url = `${WS_BASE}/${this.symbol}@aggTrade`;
    this.tradeSocket = new WebSocket(url);

    this.tradeSocket.on('open', () => {
      this.onTradeConnected?.({ symbol: this.symbol, stream: `${this.symbol}@aggTrade` });
    });

    this.tradeSocket.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      const trade = {
        trade_id: msg.a,
        symbol: msg.s,
        price: Number(msg.p),
        quantity: Number(msg.q),
        trade_time: msg.T,
        maker_flag: msg.m ? 1 : 0,
        side: msg.m ? 'sell' : 'buy',
        ingest_ts: Date.now()
      };
      this.onTrade?.(trade);
    });

    this.tradeSocket.on('close', () => this.scheduleReconnect('trade'));
    this.tradeSocket.on('error', () => this.tradeSocket?.close());
  }

  connectBookTicker() {
    const url = `${WS_BASE}/${this.symbol}@bookTicker`;
    this.bookSocket = new WebSocket(url);

    this.bookSocket.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      const book = {
        symbol: msg.s,
        bid_price: Number(msg.b),
        bid_qty: Number(msg.B),
        ask_price: Number(msg.a),
        ask_qty: Number(msg.A),
        ts: msg.T || Date.now()
      };
      this.onBookTicker?.(book);
    });

    this.bookSocket.on('close', () => this.scheduleReconnect('book'));
    this.bookSocket.on('error', () => this.bookSocket?.close());
  }

  connectDepth() {
    this.depthReady = false;
    this.depthBuffer = [];
    this.lastUpdateId = null;

    const url = `${WS_BASE}/${this.symbol}@depth@100ms`;
    this.depthSocket = new WebSocket(url);

    this.depthSocket.on('open', () => {
      void this.refreshDepthSnapshot({ reason: 'socket-open' });
    });

    this.depthSocket.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (!this.depthReady) {
        this.depthBuffer.push(msg);
        if (this.depthBuffer.length > 4000) this.depthBuffer.shift();
        return;
      }
      this.processDepthMessage(msg);
    });

    this.depthSocket.on('close', () => {
      clearTimeout(this.depthSnapshotRetryTimer);
      this.scheduleReconnect('depth');
    });
    this.depthSocket.on('error', () => this.depthSocket?.close());
  }

  connectKline() {
    const url = `${WS_BASE}/${this.symbol}@kline_1m`;
    this.klineSocket = new WebSocket(url);

    this.klineSocket.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      const kline = msg.k;
      if (!kline) return;
      this.onCandle?.({
        time: Math.floor(kline.t / 1000),
        open: Number(kline.o),
        high: Number(kline.h),
        low: Number(kline.l),
        close: Number(kline.c),
        volume: Number(kline.v),
        isFinal: kline.x
      });
    });

    this.klineSocket.on('close', () => this.scheduleReconnect('kline'));
    this.klineSocket.on('error', () => this.klineSocket?.close());
  }

  async loadDepthSnapshot() {
    if (this.resyncInFlight) return false;
    this.resyncInFlight = true;

    let retryAfterMs = DEPTH_SNAPSHOT_RETRY_MS;
    let lastFailure = null;

    try {
      for (const baseUrl of REST_BASES) {
        const url = `${baseUrl}/depth?symbol=${this.symbol.toUpperCase()}&limit=1000`;

        try {
          const response = await fetch(url);
          if (!response.ok) {
            const responseBody = await response.text();
            retryAfterMs = Math.max(retryAfterMs, parseRetryAfterMs(responseBody));
            lastFailure = { status: response.status, url, responseBody };
            console.warn('[depth] snapshot request rejected', lastFailure);
            continue;
          }

          const snapshot = await response.json();
          this.bids.clear();
          this.asks.clear();

          snapshot.bids.forEach(([price, quantity]) => {
            const p = Number(price);
            const q = Number(quantity);
            if (q > 0) this.bids.set(p, q);
          });

          snapshot.asks.forEach(([price, quantity]) => {
            const p = Number(price);
            const q = Number(quantity);
            if (q > 0) this.asks.set(p, q);
          });

          this.lastUpdateId = snapshot.lastUpdateId;
          this.depthReady = true;
          clearTimeout(this.depthSnapshotRetryTimer);
          return true;
        } catch (error) {
          lastFailure = { url, error: error?.message || String(error) };
          console.warn('[depth] snapshot request failed', lastFailure);
        }
      }

      this.depthReady = false;
      console.warn('[depth] snapshot unavailable; continuing without order book until retry succeeds', lastFailure);
      this.scheduleDepthSnapshotRetry(retryAfterMs);
      return false;
    } finally {
      this.resyncInFlight = false;
    }
  }

  async refreshDepthSnapshot({ reason = 'retry' } = {}) {
    const loaded = await this.loadDepthSnapshot();
    if (!loaded) return false;

    const bufferedEventCount = this.depthBuffer.length;
    this.flushDepthBuffer();
    this.emitDepth(true);
    console.info('[depth] snapshot synchronized', {
      reason,
      bufferedEventCount,
      lastUpdateId: this.lastUpdateId
    });
    return true;
  }

  scheduleDepthSnapshotRetry(delayMs = DEPTH_SNAPSHOT_RETRY_MS) {
    clearTimeout(this.depthSnapshotRetryTimer);
    this.depthSnapshotRetryTimer = setTimeout(() => {
      if (this.depthSocket?.readyState !== WebSocket.OPEN || this.depthReady) return;
      void this.refreshDepthSnapshot({ reason: 'scheduled-retry' });
    }, Math.max(1_000, delayMs));
  }

  flushDepthBuffer() {
    if (!this.depthReady) return;
    const bufferedMessages = [...this.depthBuffer];
    this.depthBuffer = [];
    bufferedMessages.forEach((msg) => this.processDepthMessage(msg));
  }

  processDepthMessage(msg) {
    if (!this.depthReady || this.lastUpdateId === null) return;

    if (msg.u <= this.lastUpdateId) return;

    if (msg.U > this.lastUpdateId + 1) {
      void this.resyncDepth();
      return;
    }

    this.applyDepthSide(this.bids, msg.b);
    this.applyDepthSide(this.asks, msg.a);
    this.lastUpdateId = msg.u;
    this.emitDepth();
  }

  applyDepthSide(sideMap, updates = []) {
    updates.forEach(([price, quantity]) => {
      const p = Number(price);
      const q = Number(quantity);
      if (q === 0) sideMap.delete(p);
      else sideMap.set(p, q);
    });
  }

  async resyncDepth() {
    if (this.resyncInFlight) return false;
    this.depthReady = false;
    this.depthBuffer = [];
    this.lastUpdateId = null;
    return this.refreshDepthSnapshot({ reason: 'stream-gap' });
  }

  emitDepth(force = false) {
    const now = Date.now();
    if (!force && now - this.lastDepthEmit < 120) {
      clearTimeout(this.depthEmitTimer);
      this.depthEmitTimer = setTimeout(() => this.emitDepth(true), 130);
      return;
    }

    this.lastDepthEmit = now;
    const payload = buildDepthPayload(this.symbol.toUpperCase(), this.bids, this.asks, now);
    this.onDepth?.(payload);
  }

  scheduleReconnect(type) {
    if (type === 'trade') {
      clearTimeout(this.tradeReconnectTimer);
      this.tradeReconnectTimer = setTimeout(() => this.connectTrade(), 1500);
    }

    if (type === 'book') {
      clearTimeout(this.bookReconnectTimer);
      this.bookReconnectTimer = setTimeout(() => this.connectBookTicker(), 1500);
    }

    if (type === 'depth') {
      clearTimeout(this.depthReconnectTimer);
      this.depthReconnectTimer = setTimeout(() => this.connectDepth(), 1500);
    }

    if (type === 'kline') {
      clearTimeout(this.klineReconnectTimer);
      this.klineReconnectTimer = setTimeout(() => this.connectKline(), 1500);
    }
  }
}

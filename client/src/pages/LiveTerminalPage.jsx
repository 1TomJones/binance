import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { io } from 'socket.io-client';
import { TopStatusBar } from '../components/TopStatusBar.jsx';
import { TradeTape } from '../components/TradeTape.jsx';
import { OrderBookLadder } from '../components/OrderBookLadder.jsx';
import { CandlestickChart } from '../components/CandlestickChart.jsx';
import { UI_LIMITS } from '../constants/uiPerformance.js';

const socket = io();

function buildStatsSnapshot(windowTrades) {
  const prices = windowTrades.map((trade) => trade.price);
  const first = prices.at(-1);
  const last = prices[0];
  return {
    first,
    last,
    high: prices.length ? Math.max(...prices) : null,
    low: prices.length ? Math.min(...prices) : null,
    movePct: first && last ? ((last - first) / first) * 100 : null
  };
}

function areStatsEqual(prev, next) {
  return prev.last === next.last
    && prev.high === next.high
    && prev.low === next.low
    && prev.movePct === next.movePct;
}

export function LiveTerminalPage() {
  const [trades, setTrades] = useState([]);
  const [book, setBook] = useState(null);
  const [depth, setDepth] = useState(null);
  const [stats, setStats] = useState({ last: null, high: null, low: null, movePct: null });
  const [connected, setConnected] = useState(socket.connected);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const rootRef = useRef(null);

  const pendingTradesRef = useRef([]);
  const statsTradesRef = useRef([]);
  const pendingBookRef = useRef(null);
  const pendingDepthRef = useRef(null);
  const frameHandleRef = useRef(null);
  const tradeDirtyRef = useRef(false);
  const bookDirtyRef = useRef(false);
  const depthDirtyRef = useRef(false);
  const statsDirtyRef = useRef(false);

  useEffect(() => {
    const flushUi = () => {
      frameHandleRef.current = null;

      if (tradeDirtyRef.current) {
        const newTrades = pendingTradesRef.current.slice().reverse();
        pendingTradesRef.current = [];
        tradeDirtyRef.current = false;

        if (newTrades.length) {
          setTrades((prev) => [...newTrades, ...prev].slice(0, UI_LIMITS.visibleTradeRows));
        }
      }

      if (bookDirtyRef.current) {
        setBook(pendingBookRef.current);
        bookDirtyRef.current = false;
      }

      if (depthDirtyRef.current) {
        setDepth(pendingDepthRef.current);
        depthDirtyRef.current = false;
      }

      if (statsDirtyRef.current) {
        const nextStats = buildStatsSnapshot(statsTradesRef.current);
        setStats((prev) => (areStatsEqual(prev, nextStats) ? prev : nextStats));
        statsDirtyRef.current = false;
      }
    };

    const scheduleUiFlush = () => {
      if (frameHandleRef.current !== null) return;
      frameHandleRef.current = window.requestAnimationFrame(flushUi);
    };

    const onConnect = () => setConnected(true);
    const onDisconnect = () => setConnected(false);

    const onBootstrap = (payload) => {
      const bootstrapTrades = payload.trades || [];
      const visibleTrades = bootstrapTrades.slice(0, UI_LIMITS.visibleTradeRows);
      setTrades(visibleTrades);

      statsTradesRef.current = bootstrapTrades.slice(0, UI_LIMITS.statsWindowSize);
      const nextStats = buildStatsSnapshot(statsTradesRef.current);
      setStats((prev) => (areStatsEqual(prev, nextStats) ? prev : nextStats));

      setBook(payload.latestBook || null);
      setDepth(payload.depth || null);
      pendingTradesRef.current = [];
      pendingBookRef.current = payload.latestBook || null;
      pendingDepthRef.current = payload.depth || null;
      tradeDirtyRef.current = false;
      bookDirtyRef.current = false;
      depthDirtyRef.current = false;
      statsDirtyRef.current = false;
    };

    const onTrade = (trade) => {
      pendingTradesRef.current.push(trade);
      if (pendingTradesRef.current.length > UI_LIMITS.tradeBufferLimit) {
        pendingTradesRef.current = pendingTradesRef.current.slice(-UI_LIMITS.tradeBufferLimit);
      }

      statsTradesRef.current = [trade, ...statsTradesRef.current].slice(0, UI_LIMITS.statsWindowSize);
      tradeDirtyRef.current = true;
      statsDirtyRef.current = true;
      scheduleUiFlush();
    };

    const onBookTicker = (nextBook) => {
      pendingBookRef.current = nextBook;
      bookDirtyRef.current = true;
      scheduleUiFlush();
    };

    const onDepth = (nextDepth) => {
      pendingDepthRef.current = nextDepth;
      depthDirtyRef.current = true;
      scheduleUiFlush();
    };

    socket.on('connect', onConnect);
    socket.on('disconnect', onDisconnect);
    socket.on('bootstrap', onBootstrap);
    socket.on('trade', onTrade);
    socket.on('bookTicker', onBookTicker);
    socket.on('depth', onDepth);

    const onFullscreenChange = () => {
      setIsFullscreen(Boolean(document.fullscreenElement));
    };

    document.addEventListener('fullscreenchange', onFullscreenChange);

    return () => {
      socket.off('connect', onConnect);
      socket.off('disconnect', onDisconnect);
      socket.off('bootstrap', onBootstrap);
      socket.off('trade', onTrade);
      socket.off('bookTicker', onBookTicker);
      socket.off('depth', onDepth);
      if (frameHandleRef.current !== null) {
        window.cancelAnimationFrame(frameHandleRef.current);
      }
      document.removeEventListener('fullscreenchange', onFullscreenChange);
    };
  }, []);

  const topBarSpread = useMemo(() => depth?.spread || (book ? book.ask_price - book.bid_price : null), [depth?.spread, book]);

  const toggleFullscreen = useCallback(async () => {
    if (!document.fullscreenElement) {
      await rootRef.current?.requestFullscreen?.();
    } else {
      await document.exitFullscreen?.();
    }
  }, []);

  return (
    <main className="terminal-root" ref={rootRef}>
      <TopStatusBar
        mode="LIVE"
        symbol="BTCUSDT"
        lastPrice={stats.last}
        high={stats.high}
        low={stats.low}
        movePct={stats.movePct}
        bid={depth?.bestBid?.price || book?.bid_price}
        ask={depth?.bestAsk?.price || book?.ask_price}
        spread={topBarSpread}
        connected={connected}
        onToggleFullscreen={toggleFullscreen}
        isFullscreen={isFullscreen}
      />
      <section className="terminal-main">
        <OrderBookLadder depth={depth} />
        <div className="chart-region">
          <CandlestickChart symbol="BTCUSDT" />
        </div>
        <TradeTape trades={trades} />
      </section>
      <footer className="terminal-footer">Binance streams: BTCUSDT@depth@100ms · @trade · @kline_1m · multi-timeframe + CVD/VWAP/profile</footer>
    </main>
  );
}

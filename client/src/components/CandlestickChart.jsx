import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { io } from 'socket.io-client';
import { UI_REFRESH_INTERVALS_MS } from '../constants/uiPerformance.js';

let lightweightChartsLoader = null;
const chartSocket = io();

const defaultIndicators = {
  vwap: false,
  cvd: false,
  volumeProfile: false
};

function loadLightweightCharts() {
  if (window.LightweightCharts) return Promise.resolve(window.LightweightCharts);
  if (lightweightChartsLoader) return lightweightChartsLoader;

  lightweightChartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = 'https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js';
    script.async = true;
    script.onload = () => resolve(window.LightweightCharts);
    script.onerror = () => reject(new Error('Failed to load Lightweight Charts'));
    document.head.appendChild(script);
  });

  return lightweightChartsLoader;
}

function getLastPoint(series = []) {
  return series.length ? series[series.length - 1] : null;
}

function getSeriesSignature(series = []) {
  const first = series[0];
  const last = series[series.length - 1];
  return `${series.length}:${first?.time ?? 'na'}:${last?.time ?? 'na'}`;
}

function syncLineSeries(seriesApi, nextSeries, cacheRef) {
  if (!seriesApi) return;

  const previousSeries = cacheRef.current;
  if (!previousSeries.length || !nextSeries.length) {
    seriesApi.setData(nextSeries);
    cacheRef.current = nextSeries;
    return;
  }

  const previousLast = getLastPoint(previousSeries);
  const nextLast = getLastPoint(nextSeries);
  const appended = nextSeries.length === previousSeries.length + 1;
  const updatedTail = nextSeries.length === previousSeries.length;

  if (updatedTail && previousLast?.time === nextLast?.time) {
    seriesApi.update(nextLast);
    cacheRef.current = nextSeries;
    return;
  }

  if (appended && previousLast?.time === nextSeries[nextSeries.length - 2]?.time) {
    seriesApi.update(nextSeries[nextSeries.length - 2]);
    seriesApi.update(nextLast);
    cacheRef.current = nextSeries;
    return;
  }

  seriesApi.setData(nextSeries);
  cacheRef.current = nextSeries;
}

function syncCandleSeries(seriesApi, nextSeries, cacheRef) {
  if (!seriesApi) return;

  const previousSeries = cacheRef.current;
  if (!previousSeries.length || !nextSeries.length) {
    seriesApi.setData(nextSeries);
    cacheRef.current = nextSeries;
    return;
  }

  const previousLast = getLastPoint(previousSeries);
  const nextLast = getLastPoint(nextSeries);
  const appended = nextSeries.length === previousSeries.length + 1;
  const updatedTail = nextSeries.length === previousSeries.length;

  if (updatedTail && previousLast?.time === nextLast?.time) {
    seriesApi.update(nextLast);
    cacheRef.current = nextSeries;
    return;
  }

  if (appended && previousLast?.time === nextSeries[nextSeries.length - 2]?.time) {
    seriesApi.update(nextSeries[nextSeries.length - 2]);
    seriesApi.update(nextLast);
    cacheRef.current = nextSeries;
    return;
  }

  seriesApi.setData(nextSeries);
  cacheRef.current = nextSeries;
}

function CandlestickChartComponent({ symbol = 'BTCUSDT' }) {
  const containerRef = useRef(null);
  const overlayCanvasRef = useRef(null);
  const chartFrameRef = useRef(null);
  const lowerContainerRef = useRef(null);

  const chartRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const vwapSeriesRef = useRef(null);
  const lowerChartRef = useRef(null);
  const cvdSeriesRef = useRef(null);
  const indicatorsRef = useRef(defaultIndicators);
  const timeScaleSyncHandlerRef = useRef(null);

  const profileRef = useRef([]);
  const drawFrameRef = useRef(0);
  const snapshotTimerRef = useRef(null);
  const profileTimerRef = useRef(null);
  const activeSnapshotAbortRef = useRef(null);
  const activeProfileAbortRef = useRef(null);
  const snapshotRequestSeqRef = useRef(0);
  const profileRequestSeqRef = useRef(0);
  const refreshQueuedRef = useRef(false);
  const profileQueuedRef = useRef(false);
  const mountedRef = useRef(true);

  const candleDataRef = useRef([]);
  const vwapDataRef = useRef([]);
  const cvdDataRef = useRef([]);
  const snapshotSignatureRef = useRef('');
  const vwapSignatureRef = useRef('');
  const cvdSignatureRef = useRef('');
  const refreshSessionSnapshotRef = useRef(null);

  const [timeframe, setTimeframe] = useState('1m');
  const [menuOpen, setMenuOpen] = useState(false);
  const [indicators, setIndicators] = useState(defaultIndicators);

  const showLowerPanel = indicators.cvd;

  useEffect(() => {
    indicatorsRef.current = indicators;
  }, [indicators]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const compactLabel = useMemo(() => {
    const enabled = Object.entries(indicators).filter(([, value]) => value).map(([key]) => key);
    return enabled.length ? `Indicators (${enabled.length})` : 'Indicators';
  }, [indicators]);

  const clearVolumeProfile = useCallback(() => {
    const canvas = overlayCanvasRef.current;
    if (!canvas) return;

    canvas.width = 0;
    canvas.height = 0;
    canvas.style.width = '0px';
    canvas.style.height = '0px';
  }, []);

  const getVolumeProfilePaneBounds = useCallback(() => {
    const chart = chartRef.current;
    const frame = chartFrameRef.current;
    if (!chart || !frame) return null;

    const rightPriceScaleWidth = Math.max(Math.round(chart.priceScale('right')?.width?.() || 0), 0);
    const timeScaleHeight = Math.max(Math.round(chart.timeScale()?.height?.() || 0), 0);
    const width = Math.max(frame.clientWidth - rightPriceScaleWidth, 0);
    const height = Math.max(frame.clientHeight - timeScaleHeight, 0);

    return {
      left: 0,
      top: 0,
      width,
      height
    };
  }, []);

  const drawVolumeProfile = useCallback(() => {
    const series = candleSeriesRef.current;
    const canvas = overlayCanvasRef.current;
    const paneBounds = getVolumeProfilePaneBounds();
    const activeIndicators = indicatorsRef.current;
    const activeProfile = profileRef.current;
    const width = paneBounds?.width || 0;
    const height = paneBounds?.height || 0;

    if (!canvas || !width || !height) return;

    canvas.style.left = `${paneBounds.left}px`;
    canvas.style.top = `${paneBounds.top}px`;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;

    const pixelRatio = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * pixelRatio);
    canvas.height = Math.floor(height * pixelRatio);

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (!activeIndicators.volumeProfile || !series || !activeProfile.length) {
      return;
    }

    const rightPadding = 8;
    const maxBarWidth = width * 0.1;

    activeProfile.forEach((bucket) => {
      const topCoord = series.priceToCoordinate(bucket.price + 1);
      const bottomCoord = series.priceToCoordinate(bucket.price);
      if (!Number.isFinite(topCoord) || !Number.isFinite(bottomCoord)) return;

      const top = Math.max(Math.min(topCoord, bottomCoord), 0);
      const bottom = Math.min(Math.max(topCoord, bottomCoord), height);
      const barHeight = Math.max(bottom - top, 1);
      const barWidth = Math.max(bucket.ratio * maxBarWidth, 1);
      const left = width - rightPadding - barWidth;

      ctx.fillStyle = 'rgba(125, 173, 255, 0.5)';
      ctx.fillRect(left, top, barWidth, barHeight);
    });
  }, [getVolumeProfilePaneBounds]);

  const scheduleVolumeProfileDraw = useCallback(() => {
    if (drawFrameRef.current) window.cancelAnimationFrame(drawFrameRef.current);
    drawFrameRef.current = window.requestAnimationFrame(() => {
      drawFrameRef.current = 0;
      drawVolumeProfile();
    });
  }, [drawVolumeProfile]);

  const refreshVolumeProfile = useCallback(async ({ immediate = false } = {}) => {
    if (!indicatorsRef.current.volumeProfile) {
      profileRef.current = [];
      clearVolumeProfile();
      return;
    }

    if (activeProfileAbortRef.current && !immediate) {
      profileQueuedRef.current = true;
      return;
    }

    profileQueuedRef.current = false;
    activeProfileAbortRef.current?.abort();
    const controller = new AbortController();
    activeProfileAbortRef.current = controller;
    const requestSeq = ++profileRequestSeqRef.current;

    try {
      const response = await fetch(`/api/indicators/volume-profile?timeframe=${timeframe}`, { signal: controller.signal });
      const payload = await response.json();
      if (!mountedRef.current || controller.signal.aborted || requestSeq !== profileRequestSeqRef.current) return;
      profileRef.current = payload.profile || [];
      scheduleVolumeProfileDraw();
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('[volume-profile] refresh failed', error);
      }
    } finally {
      if (activeProfileAbortRef.current === controller) {
        activeProfileAbortRef.current = null;
      }

      if (profileQueuedRef.current && mountedRef.current) {
        profileQueuedRef.current = false;
        refreshVolumeProfile({ immediate: true });
      }
    }
  }, [clearVolumeProfile, scheduleVolumeProfileDraw, timeframe]);

  const refreshSessionSnapshot = useCallback(async ({ fit = false, immediate = false } = {}) => {
    if (activeSnapshotAbortRef.current && !immediate) {
      refreshQueuedRef.current = true;
      return;
    }

    refreshQueuedRef.current = false;
    activeSnapshotAbortRef.current?.abort();
    const controller = new AbortController();
    activeSnapshotAbortRef.current = controller;
    const requestSeq = ++snapshotRequestSeqRef.current;

    try {
      const response = await fetch(`/api/session/snapshot?timeframe=${timeframe}`, { signal: controller.signal });
      const payload = await response.json();
      if (!mountedRef.current || controller.signal.aborted || requestSeq !== snapshotRequestSeqRef.current) return;

      const candles = (payload.candles || []).map(({ time, open, high, low, close, isPlaceholder }) => {
        if (isPlaceholder || !Number.isFinite(open) || !Number.isFinite(close)) return { time };
        return { time, open, high, low, close };
      });

      const candleSignature = getSeriesSignature(candles);
      if (fit || candleSignature !== snapshotSignatureRef.current || getLastPoint(candleDataRef.current)?.close !== getLastPoint(candles)?.close) {
        syncCandleSeries(candleSeriesRef.current, candles, candleDataRef);
        snapshotSignatureRef.current = candleSignature;
      }

      if (indicatorsRef.current.vwap) {
        const vwapSeries = (payload.vwap || []).map(({ time, value }) => ({ time, value }));
        const nextSignature = getSeriesSignature(vwapSeries);
        if (fit || nextSignature !== vwapSignatureRef.current || getLastPoint(vwapDataRef.current)?.value !== getLastPoint(vwapSeries)?.value) {
          syncLineSeries(vwapSeriesRef.current, vwapSeries, vwapDataRef);
          vwapSignatureRef.current = nextSignature;
        }
      }

      if (indicatorsRef.current.cvd) {
        const cvdCandles = (payload.cvd || []).map(({ time, open, high, low, close }) => ({ time, open, high, low, close }));
        const nextSignature = getSeriesSignature(cvdCandles);
        if (fit || nextSignature !== cvdSignatureRef.current || getLastPoint(cvdDataRef.current)?.close !== getLastPoint(cvdCandles)?.close) {
          syncCandleSeries(cvdSeriesRef.current, cvdCandles, cvdDataRef);
          cvdSignatureRef.current = nextSignature;
        }
      }

      if (import.meta.env.DEV && payload.debug) {
        console.debug('[session/snapshot]', {
          timeframe,
          candles: payload.debug.sessionCandleCount,
          hydrated: payload.debug.hydratedCandleCount,
          placeholders: payload.debug.placeholderCandleCount,
          realOhlcVariance: payload.debug.realOhlcVariance,
          hydrationStatus: payload.debug.hydration?.status,
          counts: payload.debug.timeframeCounts,
          sessionStartIso: payload.sessionStartIso,
          vwapHasVariance: payload.debug.vwapHasVariance,
          cvdBarsWithTrades: payload.debug.cvdBarsWithTrades
        });
      }

      if (fit) chartRef.current?.timeScale().fitContent();
      scheduleVolumeProfileDraw();
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('[session/snapshot] refresh failed', error);
      }
    } finally {
      if (activeSnapshotAbortRef.current === controller) {
        activeSnapshotAbortRef.current = null;
      }

      if (refreshQueuedRef.current && mountedRef.current) {
        refreshQueuedRef.current = false;
        refreshSessionSnapshot({ immediate: true });
      }
    }
  }, [scheduleVolumeProfileDraw, timeframe]);

  useEffect(() => {
    refreshSessionSnapshotRef.current = refreshSessionSnapshot;
  }, [refreshSessionSnapshot]);

  useEffect(() => {
    let resizeObserver;

    loadLightweightCharts().then((lib) => {
      if (!mountedRef.current || !containerRef.current || !lib) return;

      const chart = lib.createChart(containerRef.current, {
        autoSize: true,
        layout: { background: { color: '#070d18' }, textColor: '#8fa7cc', fontFamily: 'Inter, system-ui, sans-serif' },
        grid: { vertLines: { color: 'rgba(37, 52, 79, 0.35)' }, horzLines: { color: 'rgba(37, 52, 79, 0.35)' } },
        rightPriceScale: { borderColor: '#1b2a43', scaleMargins: { top: 0.08, bottom: 0.12 } },
        timeScale: { borderColor: '#1b2a43', timeVisible: true, secondsVisible: false, rightOffset: 6, barSpacing: 9 }
      });

      const candleSeries = chart.addCandlestickSeries({
        upColor: '#27ba81', downColor: '#dc5b66', borderVisible: true, wickUpColor: '#27ba81', wickDownColor: '#dc5b66', borderUpColor: '#27ba81', borderDownColor: '#dc5b66', priceFormat: { type: 'price', precision: 2, minMove: 0.01 }
      });

      const vwapSeries = chart.addLineSeries({ color: '#93b6ff', lineWidth: 2, visible: false, lastValueVisible: false, priceLineVisible: false });

      chartRef.current = chart;
      candleSeriesRef.current = candleSeries;
      vwapSeriesRef.current = vwapSeries;

      resizeObserver = new ResizeObserver(() => {
        scheduleVolumeProfileDraw();
      });
      resizeObserver.observe(chartFrameRef.current || containerRef.current);

      if (lowerContainerRef.current) {
        const lowerChart = lib.createChart(lowerContainerRef.current, {
          autoSize: true,
          layout: { background: { color: '#070d18' }, textColor: '#8199c1', fontFamily: 'Inter, system-ui, sans-serif' },
          grid: { vertLines: { color: 'rgba(30, 43, 66, 0.2)' }, horzLines: { color: 'rgba(30, 43, 66, 0.2)' } },
          rightPriceScale: { borderColor: '#1b2a43', scaleMargins: { top: 0.18, bottom: 0.12 } },
          timeScale: { borderColor: '#1b2a43', timeVisible: true, secondsVisible: false }
        });

        const cvdSeries = lowerChart.addCandlestickSeries({
          upColor: '#d6a649',
          downColor: '#7f8eb0',
          wickUpColor: '#d6a649',
          wickDownColor: '#7f8eb0',
          borderUpColor: '#d6a649',
          borderDownColor: '#7f8eb0',
          visible: false,
          priceLineVisible: false
        });

        const syncLowerChart = (range) => {
          lowerChart.timeScale().setVisibleLogicalRange(range);
        };

        chart.timeScale().subscribeVisibleLogicalRangeChange(syncLowerChart);
        timeScaleSyncHandlerRef.current = syncLowerChart;

        lowerChartRef.current = lowerChart;
        cvdSeriesRef.current = cvdSeries;
      }

      refreshSessionSnapshotRef.current?.({ fit: true, immediate: true });
    }).catch(() => {});

    return () => {
      resizeObserver?.disconnect();
      if (timeScaleSyncHandlerRef.current && chartRef.current) {
        chartRef.current.timeScale().unsubscribeVisibleLogicalRangeChange(timeScaleSyncHandlerRef.current);
      }
      if (drawFrameRef.current) window.cancelAnimationFrame(drawFrameRef.current);
      activeSnapshotAbortRef.current?.abort();
      activeProfileAbortRef.current?.abort();
      chartRef.current?.remove();
      lowerChartRef.current?.remove();
      chartRef.current = null;
      lowerChartRef.current = null;
      candleSeriesRef.current = null;
      vwapSeriesRef.current = null;
      cvdSeriesRef.current = null;
      timeScaleSyncHandlerRef.current = null;
    };
  }, [scheduleVolumeProfileDraw]);

  useEffect(() => {
    candleDataRef.current = [];
    vwapDataRef.current = [];
    cvdDataRef.current = [];
    snapshotSignatureRef.current = '';
    vwapSignatureRef.current = '';
    cvdSignatureRef.current = '';
    refreshSessionSnapshot({ fit: true, immediate: true });
  }, [refreshSessionSnapshot]);

  useEffect(() => {
    if (!vwapSeriesRef.current) return;
    vwapSeriesRef.current.applyOptions({ visible: indicators.vwap });
    if (!indicators.vwap) {
      vwapSeriesRef.current.setData([]);
      vwapDataRef.current = [];
      vwapSignatureRef.current = '';
      return;
    }
    refreshSessionSnapshot({ immediate: true });
  }, [indicators.vwap, refreshSessionSnapshot]);

  useEffect(() => {
    if (!cvdSeriesRef.current) return;
    cvdSeriesRef.current.applyOptions({ visible: indicators.cvd });
    if (!indicators.cvd) {
      cvdSeriesRef.current.setData([]);
      cvdDataRef.current = [];
      cvdSignatureRef.current = '';
      return;
    }
    refreshSessionSnapshot({ immediate: true });
  }, [indicators.cvd, refreshSessionSnapshot]);

  useEffect(() => {
    if (!indicators.volumeProfile) {
      profileRef.current = [];
      clearVolumeProfile();
      return;
    }
    refreshVolumeProfile({ immediate: true });
  }, [clearVolumeProfile, indicators.volumeProfile, refreshVolumeProfile]);

  useEffect(() => {
    scheduleVolumeProfileDraw();
  }, [scheduleVolumeProfileDraw, showLowerPanel]);

  useEffect(() => {
    const scheduleSnapshotRefresh = () => {
      if (snapshotTimerRef.current) return;
      snapshotTimerRef.current = window.setTimeout(() => {
        snapshotTimerRef.current = null;
        refreshSessionSnapshot();
      }, UI_REFRESH_INTERVALS_MS.chartSnapshot);
    };

    const scheduleProfileRefresh = () => {
      if (!indicatorsRef.current.volumeProfile || profileTimerRef.current) return;
      profileTimerRef.current = window.setTimeout(() => {
        profileTimerRef.current = null;
        refreshVolumeProfile();
      }, UI_REFRESH_INTERVALS_MS.volumeProfile);
    };

    const onTrade = () => {
      scheduleSnapshotRefresh();
      scheduleProfileRefresh();
    };

    chartSocket.on('trade', onTrade);
    return () => {
      chartSocket.off('trade', onTrade);
      if (snapshotTimerRef.current) window.clearTimeout(snapshotTimerRef.current);
      if (profileTimerRef.current) window.clearTimeout(profileTimerRef.current);
    };
  }, [refreshSessionSnapshot, refreshVolumeProfile]);

  return (
    <div className="chart-wrap">
      <div className="chart-toolbar">
        <div className="chart-title">{symbol} · CANDLESTICK</div>
        <div className="chart-controls">
          <div className="timeframe-switcher">
            {['1m', '5m', '15m', '1h'].map((option) => (
              <button key={option} type="button" className={timeframe === option ? 'active' : ''} onClick={() => setTimeframe(option)}>{option}</button>
            ))}
          </div>
          <div className="indicator-menu-wrap">
            <button type="button" className="indicator-menu-btn" onClick={() => setMenuOpen((prev) => !prev)}>{compactLabel}</button>
            {menuOpen && (
              <div className="indicator-menu">
                {Object.keys(defaultIndicators).map((key) => (
                  <label key={key}>
                    <input type="checkbox" checked={indicators[key]} onChange={() => setIndicators((prev) => ({ ...prev, [key]: !prev[key] }))} />
                    {key.replace(/([A-Z])/g, ' $1').toUpperCase()}
                  </label>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      <div ref={chartFrameRef} className="tv-chart-frame">
        <div ref={containerRef} className="tv-chart" />
        <canvas ref={overlayCanvasRef} className="chart-overlay" />
      </div>

      <div className={`lower-panel ${showLowerPanel ? 'visible' : ''}`}>
        <div ref={lowerContainerRef} className="tv-chart lower" />
      </div>
    </div>
  );
}

export const CandlestickChart = memo(CandlestickChartComponent);

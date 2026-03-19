const DEFAULT_BINANCE_REST_BASES = [
  process.env.BINANCE_REST_URL,
  'https://api.binance.com/api/v3',
  'https://api1.binance.com/api/v3',
  'https://data-api.binance.vision/api/v3'
].filter(Boolean);

const BINANCE_AGG_TRADES_PAGE_LIMIT = 1000;
const DEFAULT_FETCH_TIMEOUT_MS = 10000;

export class HistoricalCoverageError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'HistoricalCoverageError';
    this.details = details;
  }
}

export class HistoricalBacktestDataService {
  constructor({
    getHistoricalCoverage,
    saveHistoricalCoverage,
    loadTradesByRange,
    loadLatestTradeBefore,
    getTradeStatsByRange,
    saveTradesBatch,
    fetchImpl = fetch,
    restBaseUrls = DEFAULT_BINANCE_REST_BASES,
    now = () => Date.now()
  }) {
    this.getHistoricalCoverage = getHistoricalCoverage;
    this.saveHistoricalCoverage = saveHistoricalCoverage;
    this.loadTradesByRange = loadTradesByRange;
    this.loadLatestTradeBefore = loadLatestTradeBefore;
    this.getTradeStatsByRange = getTradeStatsByRange;
    this.saveTradesBatch = saveTradesBatch;
    this.fetchImpl = fetchImpl;
    this.restBaseUrls = restBaseUrls;
    this.now = now;
  }

  async loadDay({ symbol, dayStartMs, dayEndMs, progressCallback, shouldStop }) {
    const normalizedSymbol = normalizeSymbol(symbol);
    const targetEndMs = Math.min(dayEndMs, this.now());

    progressCallback?.({ stage: 'Checking historical trade coverage' });
    await this.#ensureDayCoverage({
      symbol: normalizedSymbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      progressCallback,
      shouldStop
    });

    const trades = this.loadTradesByRange(normalizedSymbol, dayStartMs, targetEndMs, null);
    if (!trades.length) {
      throw new HistoricalCoverageError(
        `Historical trade coverage missing for ${normalizedSymbol} on ${formatUtcDay(dayStartMs)}.`,
        { symbol: normalizedSymbol, dayStartMs, dayEndMs, targetEndMs, reason: 'no_trades_after_hydration' }
      );
    }

    const seedTrade = await this.#ensureSeedTrade({
      symbol: normalizedSymbol,
      dayStartMs,
      progressCallback,
      shouldStop
    });

    return { trades, seedTrade, targetEndMs };
  }

  async #ensureDayCoverage({ symbol, dayStartMs, dayEndMs, targetEndMs, progressCallback, shouldStop }) {
    const coverage = this.getHistoricalCoverage?.(symbol, dayStartMs) || null;
    if (isCoverageSufficient(coverage, targetEndMs)) return coverage;

    // Root cause note: the replay engine used to read only whatever live websocket trades had already
    // accumulated in SQLite, which frequently meant older UTC days replayed as empty sessions.
    const fetchStartMs = determineFetchStartMs({ coverage, dayStartMs });
    const statsBefore = this.getTradeStatsByRange?.(symbol, dayStartMs, targetEndMs) || null;

    progressCallback?.({ stage: `Fetching Binance aggTrades for ${formatUtcDay(dayStartMs)}` });
    await this.#fetchAndPersistTrades({
      symbol,
      startMs: fetchStartMs,
      endMs: targetEndMs,
      progressCallback,
      shouldStop
    });

    const statsAfter = this.getTradeStatsByRange?.(symbol, dayStartMs, targetEndMs) || null;
    if (!statsAfter?.count) {
      throw new HistoricalCoverageError(
        `Unable to hydrate historical trades for ${symbol} on ${formatUtcDay(dayStartMs)}. Binance returned no usable trades.`,
        { symbol, dayStartMs, dayEndMs, targetEndMs, reason: 'empty_fetch_result', statsBefore }
      );
    }

    this.saveHistoricalCoverage?.({
      symbol,
      day_start_ms: dayStartMs,
      day_end_ms: dayEndMs,
      coverage_start_ms: dayStartMs,
      coverage_end_ms: targetEndMs,
      trade_count: Number(statsAfter.count || 0),
      first_trade_time: statsAfter.minTradeTime ?? null,
      last_trade_time: statsAfter.maxTradeTime ?? null,
      status: targetEndMs >= dayEndMs ? 'complete' : 'partial',
      source: 'binance-aggTrades'
    });

    return statsAfter;
  }

  async #ensureSeedTrade({ symbol, dayStartMs, progressCallback, shouldStop }) {
    const existing = this.loadLatestTradeBefore?.(symbol, dayStartMs) || null;
    if (existing) return existing;

    progressCallback?.({ stage: 'Fetching seed trade before session open' });
    shouldStop?.();

    const batch = await this.#fetchAggTradesPage({
      symbol,
      endTime: dayStartMs - 1,
      limit: 1,
      context: 'backtest/seed-trade'
    });

    const seedTrade = batch
      .map((trade) => normalizeAggTrade(symbol, trade))
      .filter(Boolean)
      .at(-1) || null;

    if (!seedTrade) {
      throw new HistoricalCoverageError(
        `Unable to find a seed trade before ${formatUtcDay(dayStartMs)} for ${symbol}.`,
        { symbol, dayStartMs, reason: 'missing_seed_trade' }
      );
    }

    this.saveTradesBatch?.([seedTrade]);
    return this.loadLatestTradeBefore?.(symbol, dayStartMs) || seedTrade;
  }

  async #fetchAndPersistTrades({ symbol, startMs, endMs, progressCallback, shouldStop }) {
    if (startMs > endMs) return;

    let nextFromId = null;
    let pageCount = 0;
    let lastPersistedTradeId = null;

    while (true) {
      shouldStop?.();
      pageCount += 1;
      progressCallback?.({ stage: `Hydrating trade page ${pageCount}` });

      const batch = await this.#fetchAggTradesPage({
        symbol,
        startTime: nextFromId == null ? startMs : undefined,
        endTime: nextFromId == null ? endMs : undefined,
        fromId: nextFromId,
        limit: BINANCE_AGG_TRADES_PAGE_LIMIT,
        context: 'backtest/historical-day'
      });

      if (!batch.length) break;

      const normalizedTrades = batch
        .map((trade) => normalizeAggTrade(symbol, trade))
        .filter((trade) => trade && trade.trade_time >= startMs && trade.trade_time <= endMs);

      if (normalizedTrades.length) this.saveTradesBatch?.(normalizedTrades);

      const lastTrade = batch.at(-1);
      const lastTradeId = Number(lastTrade?.a);
      const lastTradeTime = Number(lastTrade?.T);
      if (!Number.isFinite(lastTradeId) || lastTradeId === lastPersistedTradeId) {
        throw new HistoricalCoverageError(`Binance aggTrades pagination stalled for ${symbol}.`, {
          symbol,
          startMs,
          endMs,
          reason: 'pagination_stalled',
          lastTradeId,
          pageCount
        });
      }

      lastPersistedTradeId = lastTradeId;
      if (lastTradeTime >= endMs || batch.length < BINANCE_AGG_TRADES_PAGE_LIMIT) break;
      nextFromId = lastTradeId + 1;
    }
  }

  async #fetchAggTradesPage({ symbol, startTime, endTime, fromId, limit, context }) {
    const search = new URLSearchParams({
      symbol: normalizeSymbol(symbol),
      limit: String(limit || BINANCE_AGG_TRADES_PAGE_LIMIT)
    });
    if (Number.isFinite(Number(startTime))) search.set('startTime', String(startTime));
    if (Number.isFinite(Number(endTime))) search.set('endTime', String(endTime));
    if (Number.isFinite(Number(fromId))) search.set('fromId', String(fromId));
    return this.#fetchBinanceJson('/aggTrades', search, { context });
  }

  async #fetchBinanceJson(endpointPath, search, { context }) {
    let lastFailure = null;

    for (const baseUrl of this.restBaseUrls) {
      const normalizedEndpointPath = endpointPath.startsWith('/') ? endpointPath.slice(1) : endpointPath;
      const url = `${baseUrl}/${normalizedEndpointPath}?${search.toString()}`;
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), DEFAULT_FETCH_TIMEOUT_MS);

      try {
        const response = await this.fetchImpl(url, { signal: controller.signal });
        if (!response.ok) {
          lastFailure = new Error(`[${context}] Binance responded ${response.status} for ${url}`);
          continue;
        }
        return await response.json();
      } catch (error) {
        lastFailure = error;
      } finally {
        clearTimeout(timeout);
      }
    }

    throw new HistoricalCoverageError(
      `Unable to fetch historical Binance data for ${search.get('symbol') || 'unknown symbol'}.`,
      { context, cause: lastFailure?.message || String(lastFailure || 'unknown') }
    );
  }
}

function determineFetchStartMs({ coverage, dayStartMs }) {
  if (!coverage) return dayStartMs;
  const coveredUntil = Number(coverage.coverage_end_ms);
  if (!Number.isFinite(coveredUntil)) return dayStartMs;
  return Math.max(dayStartMs, coveredUntil + 1);
}

function isCoverageSufficient(coverage, targetEndMs) {
  if (!coverage || coverage.status !== 'complete' && coverage.status !== 'partial') return false;
  return Number(coverage.coverage_end_ms || 0) >= targetEndMs;
}

function normalizeAggTrade(symbol, trade) {
  const tradeId = Number(trade?.a);
  const price = Number(trade?.p);
  const quantity = Number(trade?.q);
  const tradeTime = Number(trade?.T);
  if (!Number.isFinite(tradeId) || !Number.isFinite(price) || !Number.isFinite(quantity) || !Number.isFinite(tradeTime)) {
    return null;
  }

  const makerFlag = trade?.m ? 1 : 0;
  return {
    trade_id: tradeId,
    symbol: normalizeSymbol(symbol),
    price,
    quantity,
    trade_time: tradeTime,
    maker_flag: makerFlag,
    side: makerFlag ? 'sell' : 'buy',
    ingest_ts: Date.now()
  };
}

function normalizeSymbol(symbol) {
  return String(symbol || '').toUpperCase();
}

function formatUtcDay(dayStartMs) {
  return new Date(dayStartMs).toISOString().slice(0, 10);
}

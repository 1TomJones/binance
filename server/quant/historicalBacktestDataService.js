import { Readable } from 'node:stream';
import { createInflateRaw } from 'node:zlib';

const DEFAULT_BINANCE_REST_BASES = [
  process.env.BINANCE_REST_URL,
  'https://api.binance.com/api/v3',
  'https://api1.binance.com/api/v3',
  'https://data-api.binance.vision/api/v3'
].filter(Boolean);
const DEFAULT_BINANCE_BULK_BASE_URL = process.env.BINANCE_BULK_DATA_URL || 'https://data.binance.vision';

const BINANCE_AGG_TRADES_PAGE_LIMIT = 1000;
const DEFAULT_FETCH_TIMEOUT_MS = 10000;
const DEFAULT_RETRY_ATTEMPTS = 4;
const DEFAULT_RETRY_BASE_DELAY_MS = 250;
const DEFAULT_STREAM_CHUNK_SIZE = 5000;
const DEFAULT_BULK_PERSIST_BATCH_SIZE = 5000;
const DEFAULT_SMALL_GAP_THRESHOLD_MS = 5 * 60 * 1000;

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
    loadLatestTradeInRange = null,
    getTradeStatsByRange,
    saveTradesBatch,
    streamTradesByRange = null,
    fetchImpl = fetch,
    restBaseUrls = DEFAULT_BINANCE_REST_BASES,
    bulkDataBaseUrl = DEFAULT_BINANCE_BULK_BASE_URL,
    now = () => Date.now(),
    sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    retryAttempts = DEFAULT_RETRY_ATTEMPTS,
    retryBaseDelayMs = DEFAULT_RETRY_BASE_DELAY_MS,
    streamChunkSize = DEFAULT_STREAM_CHUNK_SIZE,
    bulkPersistBatchSize = DEFAULT_BULK_PERSIST_BATCH_SIZE,
    smallGapThresholdMs = DEFAULT_SMALL_GAP_THRESHOLD_MS
  }) {
    this.getHistoricalCoverage = getHistoricalCoverage;
    this.saveHistoricalCoverage = saveHistoricalCoverage;
    this.loadTradesByRange = loadTradesByRange;
    this.loadLatestTradeBefore = loadLatestTradeBefore;
    this.loadLatestTradeInRange = loadLatestTradeInRange;
    this.getTradeStatsByRange = getTradeStatsByRange;
    this.saveTradesBatch = saveTradesBatch;
    this.streamTradesByRange = streamTradesByRange;
    this.fetchImpl = fetchImpl;
    this.restBaseUrls = restBaseUrls;
    this.bulkDataBaseUrl = bulkDataBaseUrl || null;
    this.now = now;
    this.sleep = sleep;
    this.retryAttempts = retryAttempts;
    this.retryBaseDelayMs = retryBaseDelayMs;
    this.streamChunkSize = streamChunkSize;
    this.bulkPersistBatchSize = bulkPersistBatchSize;
    this.smallGapThresholdMs = smallGapThresholdMs;
  }

  async loadDay({ symbol, dayStartMs, dayEndMs, progressCallback, shouldStop }) {
    const normalizedSymbol = normalizeSymbol(symbol);
    const targetEndMs = Math.min(dayEndMs, this.now());

    this.#emitHydrationProgress(progressCallback, {
      stage: 'Checking historical trade coverage',
      hydration: {
        source: null,
        status: 'checking',
        rowsIngested: 0,
        pagesIngested: 0,
        checkpointTimeMs: null,
        lastAggTradeId: null,
        retry: null,
        percent: 0
      }
    });

    const coverage = await this.#ensureDayCoverage({
      symbol: normalizedSymbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      progressCallback,
      shouldStop
    });

    const stats = this.getTradeStatsByRange?.(normalizedSymbol, dayStartMs, targetEndMs) || null;
    if (!Number(stats?.count || 0)) {
      throw new HistoricalCoverageError(
        `Historical trade coverage missing for ${normalizedSymbol} on ${formatUtcDay(dayStartMs)}.`,
        { symbol: normalizedSymbol, dayStartMs, dayEndMs, targetEndMs, reason: 'no_trades_after_hydration' }
      );
    }

    const trades = this.streamTradesByRange
      ? this.streamTradesByRange(normalizedSymbol, dayStartMs, targetEndMs, this.streamChunkSize)
      : this.loadTradesByRange(normalizedSymbol, dayStartMs, targetEndMs, null);

    const seedTrade = await this.#ensureSeedTrade({
      symbol: normalizedSymbol,
      dayStartMs,
      progressCallback,
      shouldStop
    });

    return {
      trades,
      seedTrade,
      targetEndMs,
      tradeCount: Number(stats?.count || 0),
      hydrationSource: coverage?.source || null,
      lastAggTradeId: coverage?.last_agg_trade_id ?? null
    };
  }

  async #ensureDayCoverage({ symbol, dayStartMs, dayEndMs, targetEndMs, progressCallback, shouldStop }) {
    const persistedCoverage = this.getHistoricalCoverage?.(symbol, dayStartMs) || null;
    const checkpoint = this.#reconcileCoverageCheckpoint({
      symbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      coverage: persistedCoverage
    });

    if (isCoverageSufficient(checkpoint, targetEndMs)) return checkpoint;

    const completedUtcDay = isCompletedUtcDay(dayEndMs, this.now());
    const gapMs = Math.max(0, targetEndMs - Number(checkpoint.coverage_end_ms || (dayStartMs - 1)));
    const preferBulk = Boolean(this.bulkDataBaseUrl) && completedUtcDay && gapMs > this.smallGapThresholdMs;

    let lastCoverage = checkpoint;
    let bulkFailure = null;

    if (preferBulk) {
      try {
        lastCoverage = await this.#hydrateWithBulkFile({
          symbol,
          dayStartMs,
          dayEndMs,
          targetEndMs,
          checkpoint,
          progressCallback,
          shouldStop
        });
      } catch (error) {
        bulkFailure = error;
        this.#emitHydrationProgress(progressCallback, {
          stage: `Bulk daily aggTrades failed; falling back to Binance REST (${error.message || error})`,
          hydration: {
            source: 'bulk-file',
            status: 'fallback',
            rowsIngested: 0,
            pagesIngested: 0,
            checkpointTimeMs: checkpoint.checkpoint_time_ms ?? checkpoint.coverage_end_ms ?? null,
            lastAggTradeId: checkpoint.last_agg_trade_id ?? null,
            retry: null,
            percent: 0
          }
        });
      }
    }

    if (!isCoverageSufficient(lastCoverage, targetEndMs)) {
      lastCoverage = await this.#hydrateWithRest({
        symbol,
        dayStartMs,
        dayEndMs,
        targetEndMs,
        checkpoint: lastCoverage,
        progressCallback,
        shouldStop,
        fallbackReason: bulkFailure?.message || null
      });
    }

    const statsAfter = this.getTradeStatsByRange?.(symbol, dayStartMs, targetEndMs) || null;
    if (!statsAfter?.count) {
      throw new HistoricalCoverageError(
        `Unable to hydrate historical trades for ${symbol} on ${formatUtcDay(dayStartMs)}. Binance returned no usable trades.`,
        {
          symbol,
          dayStartMs,
          dayEndMs,
          targetEndMs,
          reason: 'empty_fetch_result',
          bulkFailure: bulkFailure?.message || null
        }
      );
    }

    return this.#saveCoverageCheckpoint({
      symbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      source: lastCoverage?.source || (preferBulk ? 'bulk-file' : 'binance-rest'),
      stats: statsAfter,
      latestTrade: this.#loadLatestTradeInRange(symbol, dayStartMs, targetEndMs),
      status: targetEndMs >= dayEndMs ? 'complete' : 'partial'
    });
  }

  async #ensureSeedTrade({ symbol, dayStartMs, progressCallback, shouldStop }) {
    const existing = this.loadLatestTradeBefore?.(symbol, dayStartMs) || null;
    if (existing) return existing;

    this.#emitHydrationProgress(progressCallback, {
      stage: 'Fetching seed trade before session open',
      hydration: {
        source: 'binance-rest',
        status: 'seed-trade',
        rowsIngested: 0,
        pagesIngested: 1,
        checkpointTimeMs: null,
        lastAggTradeId: null,
        retry: null,
        percent: 0
      }
    });
    shouldStop?.();

    let batch;
    try {
      batch = await this.#fetchAggTradesPage({
        symbol,
        endTime: dayStartMs - 1,
        limit: 1,
        context: 'backtest/seed-trade'
      });
    } catch (error) {
      throw new HistoricalCoverageError(
        `Unable to look up a seed trade before ${formatUtcDay(dayStartMs)} for ${symbol}: ${error.message || error}.`,
        { symbol, dayStartMs, reason: 'seed_trade_lookup_failed', cause: error.message || String(error) }
      );
    }

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

  async #hydrateWithRest({ symbol, dayStartMs, dayEndMs, targetEndMs, checkpoint, progressCallback, shouldStop, fallbackReason = null }) {
    const resumeState = buildResumeState({ checkpoint, dayStartMs });
    if (resumeState.startMs > targetEndMs && resumeState.nextFromId == null) return checkpoint;

    let pageCount = 0;
    let rowsIngested = 0;
    let lastPersistedTradeId = resumeState.lastAggTradeId ?? null;
    let lastPersistedTradeTime = resumeState.checkpointTimeMs ?? null;

    while (true) {
      shouldStop?.();
      pageCount += 1;
      this.#emitHydrationProgress(progressCallback, {
        stage: `Hydrating Binance REST page ${pageCount}`,
        hydration: {
          source: 'binance-rest',
          status: 'running',
          rowsIngested,
          pagesIngested: pageCount,
          checkpointTimeMs: lastPersistedTradeTime,
          lastAggTradeId: lastPersistedTradeId,
          retry: null,
          percent: estimateRestProgressPct({ dayStartMs, targetEndMs, checkpointTimeMs: lastPersistedTradeTime })
        }
      });

      const batch = await this.#fetchAggTradesPage({
        symbol,
        startTime: resumeState.nextFromId == null ? resumeState.startMs : undefined,
        endTime: resumeState.nextFromId == null ? targetEndMs : undefined,
        fromId: resumeState.nextFromId,
        limit: BINANCE_AGG_TRADES_PAGE_LIMIT,
        context: 'backtest/historical-day',
        onRetry: ({ attempt, retryInMs, message }) => {
          this.#emitHydrationProgress(progressCallback, {
            stage: `Retrying Binance REST page ${pageCount} (attempt ${attempt}/${this.retryAttempts}) in ${retryInMs}ms`,
            hydration: {
              source: 'binance-rest',
              status: 'retrying',
              rowsIngested,
              pagesIngested: pageCount,
              checkpointTimeMs: lastPersistedTradeTime,
              lastAggTradeId: lastPersistedTradeId,
              retry: {
                attempt,
                retryInMs,
                message,
                scope: `page-${pageCount}`
              },
              percent: estimateRestProgressPct({ dayStartMs, targetEndMs, checkpointTimeMs: lastPersistedTradeTime })
            }
          });
        }
      });

      if (!batch.length) break;

      const normalizedTrades = batch
        .map((trade) => normalizeAggTrade(symbol, trade))
        .filter((trade) => trade && trade.trade_time >= dayStartMs && trade.trade_time <= targetEndMs)
        .filter((trade) => isTradeBeyondCheckpoint(trade, { tradeId: lastPersistedTradeId, tradeTime: lastPersistedTradeTime }));

      if (normalizedTrades.length) {
        this.saveTradesBatch?.(normalizedTrades);
        rowsIngested += normalizedTrades.length;
        const checkpointTrade = normalizedTrades.at(-1);
        lastPersistedTradeId = checkpointTrade.trade_id;
        lastPersistedTradeTime = checkpointTrade.trade_time;
        this.#saveCoverageCheckpoint({
          symbol,
          dayStartMs,
          dayEndMs,
          targetEndMs,
          source: fallbackReason ? 'binance-rest-fallback' : 'binance-rest',
          latestTrade: checkpointTrade,
          status: lastPersistedTradeTime >= targetEndMs ? 'complete' : 'partial'
        });
      }

      const lastTrade = batch.at(-1);
      const lastTradeId = Number(lastTrade?.a);
      const lastTradeTime = Number(lastTrade?.T);
      if (!Number.isFinite(lastTradeId) || lastTradeId === resumeState.lastSeenRemoteTradeId) {
        throw new HistoricalCoverageError(`Binance aggTrades pagination stalled for ${symbol}.`, {
          symbol,
          dayStartMs,
          dayEndMs,
          targetEndMs,
          reason: 'pagination_stalled',
          lastTradeId,
          pageCount
        });
      }

      resumeState.lastSeenRemoteTradeId = lastTradeId;
      if (lastTradeTime >= targetEndMs || batch.length < BINANCE_AGG_TRADES_PAGE_LIMIT) break;
      resumeState.nextFromId = lastTradeId + 1;
      resumeState.startMs = lastPersistedTradeTime != null ? lastPersistedTradeTime + 1 : Math.max(dayStartMs, resumeState.startMs);
    }

    return this.getHistoricalCoverage?.(symbol, dayStartMs) || checkpoint;
  }

  async #hydrateWithBulkFile({ symbol, dayStartMs, dayEndMs, targetEndMs, checkpoint, progressCallback, shouldStop }) {
    const url = buildBulkAggTradesUrl(this.bulkDataBaseUrl, symbol, dayStartMs);
    const response = await this.#fetchBulkFile(url);
    const contentLength = Number(response.headers?.get?.('content-length') || 0);
    const resumeState = buildResumeState({ checkpoint, dayStartMs });
    const lineState = {
      bufferedText: '',
      rowsSeen: 0,
      rowsPersisted: 0,
      compressedBytesRead: 0,
      lastTradeId: resumeState.lastAggTradeId ?? null,
      lastTradeTime: resumeState.checkpointTimeMs ?? null,
      batch: []
    };

    this.#emitHydrationProgress(progressCallback, {
      stage: `Streaming Binance bulk aggTrades ZIP for ${formatUtcDay(dayStartMs)}`,
      hydration: {
        source: 'bulk-file',
        status: 'running',
        rowsIngested: 0,
        pagesIngested: 1,
        checkpointTimeMs: lineState.lastTradeTime,
        lastAggTradeId: lineState.lastTradeId,
        retry: null,
        percent: 0
      }
    });

    const entryStream = await streamZipEntryFromBody(response.body, {
      shouldStop,
      onCompressedBytes: (byteCount) => {
        lineState.compressedBytesRead += byteCount;
        this.#emitHydrationProgress(progressCallback, {
          stage: `Streaming Binance bulk aggTrades ZIP for ${formatUtcDay(dayStartMs)}`,
          hydration: {
            source: 'bulk-file',
            status: 'running',
            rowsIngested: lineState.rowsPersisted,
            pagesIngested: 1,
            checkpointTimeMs: lineState.lastTradeTime,
            lastAggTradeId: lineState.lastTradeId,
            retry: null,
            percent: estimateBulkProgressPct({ compressedBytesRead: lineState.compressedBytesRead, contentLength })
          }
        });
      }
    });

    const decoder = new TextDecoder();
    for await (const chunk of entryStream) {
      shouldStop?.();
      lineState.bufferedText += decoder.decode(chunk, { stream: true });
      const lines = lineState.bufferedText.split('\n');
      lineState.bufferedText = lines.pop() || '';
      for (const line of lines) {
        const trade = parseBulkAggTradeLine(symbol, line);
        if (!trade) continue;
        lineState.rowsSeen += 1;
        if (trade.trade_time < dayStartMs || trade.trade_time > targetEndMs) continue;
        if (!isTradeBeyondCheckpoint(trade, { tradeId: resumeState.lastAggTradeId, tradeTime: resumeState.checkpointTimeMs })) continue;
        lineState.batch.push(trade);
        lineState.lastTradeId = trade.trade_id;
        lineState.lastTradeTime = trade.trade_time;
        if (lineState.batch.length >= this.bulkPersistBatchSize) {
          this.#flushBulkBatch({
            symbol,
            dayStartMs,
            dayEndMs,
            targetEndMs,
            state: lineState,
            progressCallback
          });
        }
      }
    }

    lineState.bufferedText += decoder.decode();
    if (lineState.bufferedText.trim()) {
      const trade = parseBulkAggTradeLine(symbol, lineState.bufferedText);
      if (trade && trade.trade_time >= dayStartMs && trade.trade_time <= targetEndMs
        && isTradeBeyondCheckpoint(trade, { tradeId: resumeState.lastAggTradeId, tradeTime: resumeState.checkpointTimeMs })) {
        lineState.batch.push(trade);
        lineState.lastTradeId = trade.trade_id;
        lineState.lastTradeTime = trade.trade_time;
      }
    }

    this.#flushBulkBatch({
      symbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      state: lineState,
      progressCallback,
      forceStatus: lineState.lastTradeTime >= targetEndMs ? 'complete' : 'partial'
    });

    if (lineState.rowsPersisted === 0 && !isCoverageSufficient(checkpoint, targetEndMs)) {
      throw new HistoricalCoverageError(
        `Bulk aggTrades file returned no usable trades for ${symbol} on ${formatUtcDay(dayStartMs)}.`,
        { symbol, dayStartMs, dayEndMs, targetEndMs, reason: 'bulk_empty' }
      );
    }

    return this.getHistoricalCoverage?.(symbol, dayStartMs) || checkpoint;
  }

  #flushBulkBatch({ symbol, dayStartMs, dayEndMs, targetEndMs, state, progressCallback, forceStatus = null }) {
    if (!state.batch.length) return;
    this.saveTradesBatch?.(state.batch);
    state.rowsPersisted += state.batch.length;
    const latestTrade = state.batch.at(-1);
    this.#saveCoverageCheckpoint({
      symbol,
      dayStartMs,
      dayEndMs,
      targetEndMs,
      source: 'bulk-file',
      latestTrade,
      status: forceStatus || (latestTrade.trade_time >= targetEndMs ? 'complete' : 'partial')
    });
    state.batch = [];
    this.#emitHydrationProgress(progressCallback, {
      stage: `Checkpointed Binance bulk aggTrades through trade ${latestTrade.trade_id}`,
      hydration: {
        source: 'bulk-file',
        status: 'running',
        rowsIngested: state.rowsPersisted,
        pagesIngested: 1,
        checkpointTimeMs: latestTrade.trade_time,
        lastAggTradeId: latestTrade.trade_id,
        retry: null,
        percent: 100
      }
    });
  }

  async #fetchAggTradesPage({ symbol, startTime, endTime, fromId, limit, context, onRetry }) {
    const search = new URLSearchParams({
      symbol: normalizeSymbol(symbol),
      limit: String(limit || BINANCE_AGG_TRADES_PAGE_LIMIT)
    });
    if (isFiniteQueryValue(startTime)) search.set('startTime', String(startTime));
    if (isFiniteQueryValue(endTime)) search.set('endTime', String(endTime));
    if (isFiniteQueryValue(fromId)) search.set('fromId', String(fromId));
    return this.#fetchBinanceJson('/aggTrades', search, { context, onRetry });
  }

  async #fetchBinanceJson(endpointPath, search, { context, onRetry }) {
    let lastFailure = null;

    for (const baseUrl of this.restBaseUrls) {
      const normalizedEndpointPath = endpointPath.startsWith('/') ? endpointPath.slice(1) : endpointPath;
      const url = `${baseUrl}/${normalizedEndpointPath}?${search.toString()}`;

      for (let attempt = 1; attempt <= this.retryAttempts; attempt += 1) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), DEFAULT_FETCH_TIMEOUT_MS);

        try {
          const response = await this.fetchImpl(url, { signal: controller.signal });
          if (!response.ok) {
            const bodyPreview = await readBodyPreview(response);
            const responseError = new Error(buildHttpFailureMessage({ context, status: response.status, url, bodyPreview }));
            const retryable = isRetryableStatus(response.status);
            lastFailure = responseError;
            if (retryable && attempt < this.retryAttempts) {
              const retryInMs = resolveRetryDelayMs(response, attempt, this.retryBaseDelayMs);
              onRetry?.({ attempt, retryInMs, message: responseError.message });
              await this.sleep(retryInMs);
              continue;
            }
            if (!retryable) break;
            continue;
          }

          const payload = await response.json();
          if (!Array.isArray(payload)) {
            throw new Error(`[${context}] Binance returned a non-array payload for ${url}`);
          }
          return payload;
        } catch (error) {
          lastFailure = error;
          if (!isRetryableError(error) || attempt >= this.retryAttempts) break;
          const retryInMs = resolveRetryDelayMs(null, attempt, this.retryBaseDelayMs);
          onRetry?.({ attempt, retryInMs, message: error.message || String(error) });
          await this.sleep(retryInMs);
        } finally {
          clearTimeout(timeout);
        }
      }
    }

    throw new HistoricalCoverageError(
      `Unable to fetch historical Binance data for ${search.get('symbol') || 'unknown symbol'}: ${lastFailure?.message || String(lastFailure || 'unknown')}`,
      { context, cause: lastFailure?.message || String(lastFailure || 'unknown') }
    );
  }

  async #fetchBulkFile(url) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DEFAULT_FETCH_TIMEOUT_MS);
    try {
      const response = await this.fetchImpl(url, { signal: controller.signal });
      if (!response.ok || !response.body) {
        const bodyPreview = await readBodyPreview(response);
        throw new HistoricalCoverageError(
          `Unable to fetch Binance bulk aggTrades ZIP: ${buildHttpFailureMessage({ context: 'backtest/bulk-aggTrades', status: response.status, url, bodyPreview })}`,
          { url, status: response.status }
        );
      }
      return response;
    } catch (error) {
      if (error instanceof HistoricalCoverageError) throw error;
      throw new HistoricalCoverageError(
        `Unable to fetch Binance bulk aggTrades ZIP for ${url}: ${error.message || error}`,
        { url, cause: error.message || String(error) }
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  #reconcileCoverageCheckpoint({ symbol, dayStartMs, dayEndMs, targetEndMs, coverage }) {
    const latestTrade = this.#loadLatestTradeInRange(symbol, dayStartMs, targetEndMs);
    const stats = this.getTradeStatsByRange?.(symbol, dayStartMs, targetEndMs) || null;
    const latestTradeTime = Number(latestTrade?.trade_time);
    const coverageEndMs = Math.max(
      Number(coverage?.coverage_end_ms || (dayStartMs - 1)),
      Number.isFinite(latestTradeTime) ? latestTradeTime : (dayStartMs - 1)
    );
    const lastAggTradeId = toFiniteOrNull(latestTrade?.trade_id)
      ?? toFiniteOrNull(coverage?.last_agg_trade_id);
    const status = coverageEndMs >= targetEndMs
      ? (targetEndMs >= dayEndMs ? 'complete' : 'partial')
      : (coverage?.status || 'partial');

    return {
      symbol,
      day_start_ms: dayStartMs,
      day_end_ms: dayEndMs,
      coverage_start_ms: dayStartMs,
      coverage_end_ms: coverageEndMs,
      trade_count: Number(stats?.count || coverage?.trade_count || 0),
      first_trade_time: stats?.minTradeTime ?? coverage?.first_trade_time ?? null,
      last_trade_time: stats?.maxTradeTime ?? coverage?.last_trade_time ?? latestTrade?.trade_time ?? null,
      last_agg_trade_id: lastAggTradeId,
      checkpoint_time_ms: Number.isFinite(latestTradeTime)
        ? latestTradeTime
        : toFiniteOrNull(coverage?.checkpoint_time_ms),
      checkpoint_rows: Number(stats?.count || coverage?.checkpoint_rows || 0),
      status,
      source: coverage?.source || (latestTrade ? 'sqlite-reconciled' : 'unknown')
    };
  }

  #saveCoverageCheckpoint({ symbol, dayStartMs, dayEndMs, targetEndMs, source, latestTrade = null, stats = null, status = 'partial' }) {
    const effectiveStats = stats || this.getTradeStatsByRange?.(symbol, dayStartMs, targetEndMs) || null;
    const effectiveLatestTrade = latestTrade || this.#loadLatestTradeInRange(symbol, dayStartMs, targetEndMs) || null;
    const record = {
      symbol,
      day_start_ms: dayStartMs,
      day_end_ms: dayEndMs,
      coverage_start_ms: dayStartMs,
      coverage_end_ms: Math.max(Number(effectiveLatestTrade?.trade_time || (dayStartMs - 1)), dayStartMs - 1),
      trade_count: Number(effectiveStats?.count || 0),
      first_trade_time: effectiveStats?.minTradeTime ?? null,
      last_trade_time: effectiveStats?.maxTradeTime ?? null,
      last_agg_trade_id: effectiveLatestTrade?.trade_id ?? null,
      checkpoint_time_ms: effectiveLatestTrade?.trade_time ?? null,
      checkpoint_rows: Number(effectiveStats?.count || 0),
      status,
      source
    };
    return this.saveHistoricalCoverage?.(record) || record;
  }

  #loadLatestTradeInRange(symbol, startMs, endMs) {
    if (typeof this.loadLatestTradeInRange === 'function') {
      return this.loadLatestTradeInRange(symbol, startMs, endMs) || null;
    }
    return this.loadTradesByRange?.(symbol, startMs, endMs, 1_000)?.at?.(-1) || null;
  }

  #emitHydrationProgress(progressCallback, { stage, hydration }) {
    progressCallback?.({
      phase: 'hydration',
      stage,
      hydration: {
        source: hydration?.source || null,
        status: hydration?.status || 'running',
        rowsIngested: Number(hydration?.rowsIngested || 0),
        pagesIngested: Number(hydration?.pagesIngested || 0),
        checkpointTimeMs: hydration?.checkpointTimeMs ?? null,
        lastAggTradeId: hydration?.lastAggTradeId ?? null,
        retry: hydration?.retry || null,
        percent: Number.isFinite(Number(hydration?.percent)) ? Number(hydration.percent) : null
      }
    });
  }
}

function buildResumeState({ checkpoint, dayStartMs }) {
  const checkpointTimeMs = toFiniteOrNull(checkpoint?.checkpoint_time_ms)
    ?? toFiniteOrNull(checkpoint?.coverage_end_ms);
  const lastAggTradeId = toFiniteOrNull(checkpoint?.last_agg_trade_id);
  return {
    checkpointTimeMs,
    lastAggTradeId,
    startMs: checkpointTimeMs != null ? Math.max(dayStartMs, checkpointTimeMs + 1) : dayStartMs,
    nextFromId: lastAggTradeId != null ? lastAggTradeId + 1 : null,
    lastSeenRemoteTradeId: null
  };
}

function isCoverageSufficient(coverage, targetEndMs) {
  if (!coverage || (coverage.status !== 'complete' && coverage.status !== 'partial')) return false;
  return Number(coverage.coverage_end_ms || 0) >= targetEndMs;
}

function isTradeBeyondCheckpoint(trade, { tradeId, tradeTime }) {
  if (!trade) return false;
  if (toFiniteOrNull(tradeId) != null && Number(trade.trade_id) <= Number(tradeId)) return false;
  if (toFiniteOrNull(tradeTime) != null && Number(trade.trade_time) <= Number(tradeTime) && toFiniteOrNull(tradeId) == null) return false;
  return true;
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

function parseBulkAggTradeLine(symbol, line) {
  const trimmed = String(line || '').trim();
  if (!trimmed) return null;
  const columns = trimmed.split(',');
  if (!/^\d+$/.test(String(columns[0] || '').trim())) return null;

  const tradeId = Number(columns[0]);
  const price = Number(columns[1]);
  const quantity = Number(columns[2]);
  const tradeTime = Number(columns[5]);
  const isBuyerMaker = parseBooleanCsv(columns[6]);
  if (!Number.isFinite(tradeId) || !Number.isFinite(price) || !Number.isFinite(quantity) || !Number.isFinite(tradeTime)) {
    return null;
  }

  const makerFlag = isBuyerMaker ? 1 : 0;
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

function parseBooleanCsv(value) {
  return String(value || '').trim().toLowerCase() === 'true';
}

function normalizeSymbol(symbol) {
  return String(symbol || '').toUpperCase();
}

function formatUtcDay(dayStartMs) {
  return new Date(dayStartMs).toISOString().slice(0, 10);
}

function buildBulkAggTradesUrl(baseUrl, symbol, dayStartMs) {
  const safeBase = String(baseUrl || DEFAULT_BINANCE_BULK_BASE_URL).replace(/\/$/, '');
  const normalizedSymbol = normalizeSymbol(symbol);
  return `${safeBase}/data/spot/daily/aggTrades/${normalizedSymbol}/${normalizedSymbol}-aggTrades-${formatUtcDay(dayStartMs)}.zip`;
}

function isCompletedUtcDay(dayEndMs, nowMs) {
  return dayEndMs < startOfUtcDay(nowMs);
}

function startOfUtcDay(value) {
  const date = new Date(value);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function estimateRestProgressPct({ dayStartMs, targetEndMs, checkpointTimeMs }) {
  const totalSpan = Math.max(targetEndMs - dayStartMs, 1);
  const coveredSpan = Math.max(0, Math.min(totalSpan, Number(checkpointTimeMs || dayStartMs) - dayStartMs));
  return (coveredSpan / totalSpan) * 100;
}

function estimateBulkProgressPct({ compressedBytesRead, contentLength }) {
  if (!Number.isFinite(contentLength) || contentLength <= 0) return null;
  return Math.max(0, Math.min(100, (compressedBytesRead / contentLength) * 100));
}

async function streamZipEntryFromBody(body, { shouldStop, onCompressedBytes }) {
  if (!body) throw new Error('Response body missing.');
  const reader = new WebByteReader(body);
  const localHeader = await reader.readExactly(30);
  const signature = readUInt32LE(localHeader, 0);
  if (signature !== 0x04034b50) {
    throw new Error('Unsupported ZIP payload: local file header missing.');
  }

  const flags = readUInt16LE(localHeader, 6);
  const method = readUInt16LE(localHeader, 8);
  const compressedSize = readUInt32LE(localHeader, 18);
  const fileNameLength = readUInt16LE(localHeader, 26);
  const extraLength = readUInt16LE(localHeader, 28);
  if ((flags & 0x08) !== 0) {
    throw new Error('Unsupported ZIP payload: data descriptor entries are not resumably streamable.');
  }
  if (compressedSize <= 0) {
    throw new Error('Unsupported ZIP payload: compressed size missing.');
  }

  await reader.skip(fileNameLength + extraLength);
  const compressedIterable = reader.readChunkStream(compressedSize, { shouldStop, onChunk: onCompressedBytes });
  if (method === 0) return Readable.from(compressedIterable);
  if (method !== 8) {
    throw new Error(`Unsupported ZIP compression method ${method}.`);
  }
  return Readable.from(compressedIterable).pipe(createInflateRaw());
}

class WebByteReader {
  constructor(body) {
    this.reader = typeof body.getReader === 'function'
      ? body.getReader()
      : Readable.toWeb(body).getReader();
    this.buffer = new Uint8Array(0);
    this.done = false;
  }

  async readExactly(byteCount) {
    await this.#fill(byteCount);
    if (this.buffer.length < byteCount) throw new Error('Unexpected end of ZIP stream.');
    return this.#consume(byteCount);
  }

  async skip(byteCount) {
    await this.readExactly(byteCount);
  }

  async *readChunkStream(byteCount, { shouldStop, onChunk }) {
    let remaining = byteCount;
    while (remaining > 0) {
      shouldStop?.();
      await this.#fill(1);
      if (!this.buffer.length) throw new Error('Unexpected end of ZIP payload.');
      const nextChunkSize = Math.min(remaining, this.buffer.length);
      const chunk = this.#consume(nextChunkSize);
      remaining -= chunk.length;
      onChunk?.(chunk.length);
      yield Buffer.from(chunk);
    }
  }

  async #fill(minimum) {
    while (!this.done && this.buffer.length < minimum) {
      const { value, done } = await this.reader.read();
      if (done) {
        this.done = true;
        break;
      }
      const nextValue = value instanceof Uint8Array ? value : new Uint8Array(value);
      this.buffer = concatUint8(this.buffer, nextValue);
    }
  }

  #consume(byteCount) {
    const chunk = this.buffer.slice(0, byteCount);
    this.buffer = this.buffer.slice(byteCount);
    return chunk;
  }
}

function concatUint8(left, right) {
  const combined = new Uint8Array(left.length + right.length);
  combined.set(left, 0);
  combined.set(right, left.length);
  return combined;
}

function readUInt16LE(bytes, offset) {
  return bytes[offset] | (bytes[offset + 1] << 8);
}

function readUInt32LE(bytes, offset) {
  return (bytes[offset])
    | (bytes[offset + 1] << 8)
    | (bytes[offset + 2] << 16)
    | (bytes[offset + 3] << 24);
}

function isRetryableStatus(status) {
  return status === 408 || status === 409 || status === 418 || status === 429 || status >= 500;
}

function isRetryableError(error) {
  if (!error) return false;
  const name = String(error.name || '');
  return name === 'AbortError' || /timed out|timeout|ECONNRESET|ENOTFOUND|EAI_AGAIN|fetch failed/i.test(String(error.message || error));
}

function resolveRetryDelayMs(response, attempt, baseDelayMs) {
  const rawRetryAfter = response?.headers?.get?.('retry-after');
  const retryAfter = rawRetryAfter == null || rawRetryAfter === '' ? null : Number(rawRetryAfter);
  if (Number.isFinite(retryAfter) && retryAfter >= 0) return retryAfter * 1000;
  return baseDelayMs * (2 ** Math.max(attempt - 1, 0));
}

async function readBodyPreview(response) {
  try {
    const body = await response.text();
    return String(body || '').slice(0, 240);
  } catch {
    return '';
  }
}

function buildHttpFailureMessage({ context, status, url, bodyPreview }) {
  const details = bodyPreview ? ` ${bodyPreview}` : '';
  return `[${context}] Binance responded ${status} for ${url}.${details}`.trim();
}

function isFiniteQueryValue(value) {
  return value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
}

function toFiniteOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

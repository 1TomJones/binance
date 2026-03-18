import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dataDir = path.resolve('data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(path.join(dataDir, 'terminal.db'));

db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    trade_time INTEGER NOT NULL,
    maker_flag INTEGER NOT NULL,
    side TEXT NOT NULL,
    ingest_ts INTEGER NOT NULL,
    UNIQUE(symbol, trade_id)
  );

  CREATE TABLE IF NOT EXISTS book_ticker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    bid_price REAL NOT NULL,
    bid_qty REAL NOT NULL,
    ask_price REAL NOT NULL,
    ask_qty REAL NOT NULL,
    ts INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS quant_strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL,
    raw_content TEXT NOT NULL,
    parse_status TEXT NOT NULL,
    metadata_json TEXT,
    parse_message TEXT,
    created_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS quant_backtest_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id INTEGER,
    status TEXT NOT NULL,
    progress_pct INTEGER NOT NULL DEFAULT 0,
    processed_items INTEGER NOT NULL DEFAULT 0,
    current_marker TEXT,
    elapsed_ms INTEGER,
    run_config_json TEXT,
    result_id INTEGER,
    error_message TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS quant_backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    summary_json TEXT,
    equity_series_json TEXT,
    trade_log_json TEXT,
    created_at INTEGER NOT NULL
  );



  CREATE TABLE IF NOT EXISTS quant_live_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS quant_job_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    progress_pct INTEGER NOT NULL,
    processed_items INTEGER,
    current_marker TEXT,
    elapsed_ms INTEGER,
    ts INTEGER NOT NULL
  );
`);

const insertTradeStmt = db.prepare(`
  INSERT OR IGNORE INTO trades (
    trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
  ) VALUES (
    @trade_id, @symbol, @price, @quantity, @trade_time, @maker_flag, @side, @ingest_ts
  )
`);

const insertTradesManyStmt = db.transaction((trades) => {
  for (const trade of trades) insertTradeStmt.run(trade);
});

const insertBookStmt = db.prepare(`
  INSERT INTO book_ticker (
    symbol, bid_price, bid_qty, ask_price, ask_qty, ts
  ) VALUES (
    @symbol, @bid_price, @bid_qty, @ask_price, @ask_qty, @ts
  )
`);

const insertStrategyStmt = db.prepare(`
  INSERT INTO quant_strategies (file_name, raw_content, parse_status, metadata_json, parse_message, created_at)
  VALUES (@file_name, @raw_content, @parse_status, @metadata_json, @parse_message, @created_at)
`);

const createJobStmt = db.prepare(`
  INSERT INTO quant_backtest_jobs (
    strategy_id, status, progress_pct, processed_items, current_marker, run_config_json, created_at, updated_at
  ) VALUES (
    @strategy_id, 'queued', 0, 0, 'Queued', @run_config_json, @created_at, @updated_at
  )
`);

const updateJobStmt = db.prepare(`
  UPDATE quant_backtest_jobs
  SET status = COALESCE(@status, status),
      progress_pct = COALESCE(@progress_pct, progress_pct),
      processed_items = COALESCE(@processed_items, processed_items),
      current_marker = COALESCE(@current_marker, current_marker),
      elapsed_ms = COALESCE(@elapsed_ms, elapsed_ms),
      result_id = COALESCE(@result_id, result_id),
      error_message = COALESCE(@error_message, error_message),
      updated_at = @updated_at
  WHERE id = @id
`);

const insertResultStmt = db.prepare(`
  INSERT INTO quant_backtest_results (job_id, summary_json, equity_series_json, trade_log_json, created_at)
  VALUES (@job_id, @summary_json, @equity_series_json, @trade_log_json, @created_at)
`);

const insertProgressStmt = db.prepare(`
  INSERT INTO quant_job_progress (job_id, status, progress_pct, processed_items, current_marker, elapsed_ms, ts)
  VALUES (@job_id, @status, @progress_pct, @processed_items, @current_marker, @elapsed_ms, @ts)
`);

const upsertLiveRunStmt = db.prepare(`
  INSERT INTO quant_live_runs (strategy_id, status, state_json, updated_at)
  VALUES (@strategy_id, @status, @state_json, @updated_at)
  ON CONFLICT(strategy_id) DO UPDATE SET
    status = excluded.status,
    state_json = excluded.state_json,
    updated_at = excluded.updated_at
`);

const QUANT_BACKTEST_JOB_UPDATE_DEFAULTS = Object.freeze({
  status: null,
  progress_pct: null,
  processed_items: null,
  current_marker: null,
  elapsed_ms: null,
  result_id: null,
  error_message: null
});

function normalizeQuantBacktestJobPatch(patch = {}) {
  return {
    ...QUANT_BACKTEST_JOB_UPDATE_DEFAULTS,
    ...(patch || {})
  };
}

export function saveTrade(trade) {
  insertTradeStmt.run(trade);
}

export function saveTradesBatch(trades) {
  if (!trades?.length) return;
  insertTradesManyStmt(trades);
}

export function getTradesCountByRange(symbol, start, end) {
  return db.prepare(`
    SELECT COUNT(*) as count
    FROM trades
    WHERE symbol = ? AND trade_time BETWEEN ? AND ?
  `).get(symbol, start, end)?.count || 0;
}

export function saveBookTicker(book) {
  insertBookStmt.run(book);
}

export function getRecentTrades(symbol, limit = 500) {
  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
    ORDER BY trade_time DESC
    LIMIT ?
  `).all(symbol, limit);
}

export function getTradeRange(symbol) {
  return db.prepare(`
    SELECT MIN(trade_time) as minTime, MAX(trade_time) as maxTime, COUNT(*) as count
    FROM trades
    WHERE symbol = ?
  `).get(symbol);
}

export function getTradesByRange(symbol, start, end, limit = 20000) {
  if (limit == null) {
    return db.prepare(`
      SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
      FROM trades
      WHERE symbol = ?
        AND trade_time BETWEEN ? AND ?
      ORDER BY trade_time ASC
    `).all(symbol, start, end);
  }

  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
      AND trade_time BETWEEN ? AND ?
    ORDER BY trade_time ASC
    LIMIT ?
  `).all(symbol, start, end, limit);
}

export function getLatestBook(symbol) {
  return db.prepare(`
    SELECT symbol, bid_price, bid_qty, ask_price, ask_qty, ts
    FROM book_ticker
    WHERE symbol = ?
    ORDER BY ts DESC
    LIMIT 1
  `).get(symbol);
}

export function saveQuantStrategy(record) {
  const now = Date.now();
  const info = insertStrategyStmt.run({ ...record, created_at: now });
  return getQuantStrategyById(info.lastInsertRowid);
}

export function getQuantStrategyById(id) {
  return db.prepare('SELECT * FROM quant_strategies WHERE id = ?').get(id);
}

export function listQuantStrategies(limit = 100) {
  return db.prepare(`
    SELECT * FROM quant_strategies
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limit);
}

export function createQuantBacktestJob(record) {
  const now = Date.now();
  const info = createJobStmt.run({ ...record, created_at: now, updated_at: now });
  const job = getQuantBacktestJobById(info.lastInsertRowid);
  saveQuantJobProgress({
    job_id: job.id,
    status: job.status,
    progress_pct: job.progress_pct,
    processed_items: job.processed_items,
    current_marker: job.current_marker,
    elapsed_ms: job.elapsed_ms || 0
  });
  return job;
}

export function updateQuantBacktestJob(id, patch = {}) {
  const normalizedPatch = normalizeQuantBacktestJobPatch(patch);
  updateJobStmt.run({ id, ...normalizedPatch, updated_at: Date.now() });
  const job = getQuantBacktestJobById(id);
  if (!job) return null;
  saveQuantJobProgress({
    job_id: id,
    status: job.status,
    progress_pct: job.progress_pct,
    processed_items: job.processed_items,
    current_marker: job.current_marker,
    elapsed_ms: job.elapsed_ms || 0
  });
  return job;
}

export function completeQuantBacktestJob(id, patch) {
  return updateQuantBacktestJob(id, { ...patch, status: 'completed', error_message: null });
}

export function failQuantBacktestJob(id, errorOrMessage, patch = {}) {
  const existingJob = getQuantBacktestJobById(id);
  const errorMessage = errorOrMessage instanceof Error
    ? (errorOrMessage.stack || errorOrMessage.message)
    : String(errorOrMessage || 'Backtest job failed.');

  return updateQuantBacktestJob(id, {
    progress_pct: existingJob?.progress_pct ?? 0,
    processed_items: existingJob?.processed_items ?? 0,
    elapsed_ms: existingJob?.elapsed_ms ?? null,
    status: 'failed',
    error_message: errorMessage,
    current_marker: patch.current_marker || 'Failed',
    ...patch
  });
}

export function getQuantBacktestJobById(id) {
  return db.prepare('SELECT * FROM quant_backtest_jobs WHERE id = ?').get(id);
}

export function listQuantBacktestJobs(limit = 50) {
  return db.prepare(`
    SELECT j.*, s.file_name as strategy_file_name, s.metadata_json
    FROM quant_backtest_jobs j
    LEFT JOIN quant_strategies s ON s.id = j.strategy_id
    ORDER BY j.created_at DESC
    LIMIT ?
  `).all(limit);
}

export function saveQuantBacktestResult(record) {
  const now = Date.now();
  const info = insertResultStmt.run({ ...record, created_at: now });
  return getQuantBacktestResultById(info.lastInsertRowid);
}

export function getQuantBacktestResultById(id) {
  return db.prepare('SELECT * FROM quant_backtest_results WHERE id = ?').get(id);
}

export function getQuantResultByJobId(jobId) {
  return db.prepare('SELECT * FROM quant_backtest_results WHERE job_id = ?').get(jobId);
}

export function saveQuantJobProgress(record) {
  insertProgressStmt.run({ ...record, ts: Date.now() });
}

export function listQuantJobProgress(jobId) {
  return db.prepare(`
    SELECT * FROM quant_job_progress
    WHERE job_id = ?
    ORDER BY ts ASC
  `).all(jobId);
}


export function saveQuantLiveRun({ strategyId, status, stateJson }) {
  upsertLiveRunStmt.run({ strategy_id: strategyId, status, state_json: stateJson, updated_at: Date.now() });
  return getQuantLiveRun(strategyId);
}

export function getQuantLiveRun(strategyId) {
  return db.prepare('SELECT * FROM quant_live_runs WHERE strategy_id = ?').get(strategyId);
}

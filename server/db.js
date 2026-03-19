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

  CREATE TABLE IF NOT EXISTS quant_live_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
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

const upsertLiveRunStmt = db.prepare(`
  INSERT INTO quant_live_runs (strategy_id, status, state_json, updated_at)
  VALUES (@strategy_id, @status, @state_json, @updated_at)
  ON CONFLICT(strategy_id) DO UPDATE SET
    status = excluded.status,
    state_json = excluded.state_json,
    updated_at = excluded.updated_at
`);

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
      ORDER BY trade_time ASC, trade_id ASC
    `).all(symbol, start, end);
  }

  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
      AND trade_time BETWEEN ? AND ?
    ORDER BY trade_time ASC, trade_id ASC
    LIMIT ?
  `).all(symbol, start, end, limit);
}

export function* streamTradesByRange(symbol, start, end, chunkSize = 5000) {
  const pageSize = Math.max(Number(chunkSize) || 0, 1);
  let cursorTradeTime = null;
  let cursorTradeId = null;

  while (true) {
    const rows = getTradesCursorPage(symbol, start, end, cursorTradeTime, cursorTradeId, pageSize);
    if (!rows.length) break;

    for (const row of rows) {
      yield row;
    }

    const lastRow = rows.at(-1);
    cursorTradeTime = Number(lastRow.trade_time);
    cursorTradeId = Number(lastRow.trade_id);
  }
}

function getTradesCursorPage(symbol, start, end, cursorTradeTime, cursorTradeId, limit) {
  if (cursorTradeTime == null || cursorTradeId == null) {
    return db.prepare(`
      SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
      FROM trades
      WHERE symbol = ?
        AND trade_time BETWEEN ? AND ?
      ORDER BY trade_time ASC, trade_id ASC
      LIMIT ?
    `).all(symbol, start, end, limit);
  }

  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
      AND trade_time BETWEEN ? AND ?
      AND (
        trade_time > ?
        OR (trade_time = ? AND trade_id > ?)
      )
    ORDER BY trade_time ASC, trade_id ASC
    LIMIT ?
  `).all(symbol, start, end, cursorTradeTime, cursorTradeTime, cursorTradeId, limit);
}

export function getLatestTradeBefore(symbol, beforeMs) {
  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
      AND trade_time < ?
    ORDER BY trade_time DESC, trade_id DESC
    LIMIT 1
  `).get(symbol, beforeMs);
}

export function getLatestTradeInRange(symbol, start, end) {
  return db.prepare(`
    SELECT trade_id, symbol, price, quantity, trade_time, maker_flag, side, ingest_ts
    FROM trades
    WHERE symbol = ?
      AND trade_time BETWEEN ? AND ?
    ORDER BY trade_time DESC, trade_id DESC
    LIMIT 1
  `).get(symbol, start, end);
}

export function getTradeStatsByRange(symbol, start, end) {
  return db.prepare(`
    SELECT COUNT(*) as count, MIN(trade_time) as minTradeTime, MAX(trade_time) as maxTradeTime
    FROM trades
    WHERE symbol = ? AND trade_time BETWEEN ? AND ?
  `).get(symbol, start, end);
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

export function saveQuantLiveRun({ strategyId, status, stateJson }) {
  upsertLiveRunStmt.run({ strategy_id: strategyId, status, state_json: stateJson, updated_at: Date.now() });
  return getQuantLiveRun(strategyId);
}

export function getQuantLiveRun(strategyId) {
  return db.prepare('SELECT * FROM quant_live_runs WHERE strategy_id = ?').get(strategyId);
}

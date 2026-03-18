# Kent Invest Crypto Tape Terminal

A unified full-stack BTCUSDT tape terminal with live Binance trade flow, trade-driven charting, and quant strategy workflows.

## Stack
- Backend: Node.js + Express + Socket.IO + better-sqlite3
- Frontend: React + Vite
- Data source: Binance public WebSocket streams (`@trade` and `@bookTicker`) via `wss://data-stream.binance.vision`
- Storage: SQLite (`data/terminal.db`)

## Features
- Live terminal mode (`/`)
  - Server-side Binance streaming and auto reconnect
  - Real-time trade tape (newest at top)
  - Trade-driven tick line chart updated on every trade
  - Last / high / low / move + bid/ask/spread in integrated top bar
- Backend persistence
  - Trades: trade ID, symbol, price, quantity, trade time, maker flag, inferred side, ingest timestamp
  - Book ticker snapshots: bid/ask price + qty and timestamp
- Quant workspace (`/quant`)
  - Upload and validate strategies
  - Run historical backtests against stored trades
  - Review performance metrics and results

## Local development
```bash
npm install
npm run dev
```
- Frontend dev server: http://localhost:5173
- Backend server: http://localhost:3000

## Production build + run
```bash
npm install
npm run build
npm start
```
Server uses `PORT` and serves `client/dist` from Express for single-service deployment.

## API endpoints
- `GET /api/health`

## Render deployment
Use a single Web Service:
- Build command: `npm install && npm run build`
- Start command: `npm start`
- Environment variable: `PORT` provided by Render

Optional `render.yaml` is included.

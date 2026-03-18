import React from 'react';
import { LiveTerminalPage } from './pages/LiveTerminalPage.jsx';
import { QuantWorkspacePage } from './pages/QuantWorkspacePage.jsx';

export function App() {
  const path = window.location.pathname;
  if (path === '/quant') return <QuantWorkspacePage />;
  return <LiveTerminalPage />;
}

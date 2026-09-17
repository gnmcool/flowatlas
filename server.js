/* =============================================================
   FLOWATLAS BACKEND v0.2 — real data, honest latency
   Zero npm dependencies. Node 18+.
   Run:  FRED_API_KEY=xxxx node server.js   (FRED key optional)
   Serves: /            -> dashboard UI
           /api/snapshot -> full state JSON
           /api/stream   -> SSE live updates
           /api/health   -> poller status + data ages

   DATA SOURCES (each field tagged with real latency):
   - Yahoo Finance v8/spark (batch)  prices, ~15-20s poll  [LIVE]
   - CoinGecko free API              crypto, ~30s poll     [LIVE]
   - NSE fiidiiTradeReact            FII/DII flows, T+1    [DAILY]
       (works from Indian residential IPs; Akamai blocks
        most cloud datacenter IPs — fails soft)
   - FRED (needs free API key)       Fed B/S, M2, HY OAS,
                                     DGS2/10/30, daily/wk  [DAILY]
   - Everything in `derived`         computed proxies      [DERIVED]
============================================================= */
require('dotenv').config();

'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');

/* A poller throwing must never take the whole process down. On a serverless host a
   crash surfaces to visitors as FUNCTION_INVOCATION_FAILED — the entire dashboard
   goes dark because one upstream API had a bad minute. Log and keep serving. */
process.on('unhandledRejection', e => console.error('[unhandledRejection]', e?.message || e));
process.on('uncaughtException',  e => console.error('[uncaughtException]',  e?.message || e));

const PORT = process.env.PORT || 8787;
const FRED_KEY = process.env.FRED_API_KEY || '';
const FINNHUB_KEY = process.env.FINNHUB_API_KEY || '';
const UA = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36' };

/* ---------------- symbol universe ---------------- */
const INDICES = [
  // US MARKETS
  { s: '^DJI', n: 'Dow Jones', cc: 'US' },
  { s: '^GSPC', n: 'S&P 500', cc: 'US', geo: 'United States of America', t12: 1, ll: [-100, 39] },
  { s: '^NDX', n: 'Nasdaq', cc: 'US' },
  // EUROPEAN MARKETS
  { s: '^FTSE', n: 'FTSE 100', cc: 'UK', geo: 'United Kingdom', t12: 1, ll: [-16, 58] },
  { s: '^FCHI', n: 'CAC 40', cc: 'FR', geo: 'France', t12: 1, ll: [-5, 44.5] },
  { s: '^GDAXI', n: 'DAX', cc: 'DE', geo: 'Germany', t12: 1, ll: [17, 52.5] },
  // ASIAN MARKETS
  { s: '^NSEI', n: 'Nifty 50', cc: 'IN', geo: 'India', t12: 1, ll: [78, 21] },
  { s: '^N225', n: 'Nikkei 225', cc: 'JP', geo: 'Japan', t12: 1, ll: [149, 41] },
  { s: '^STI', n: 'Straits Times', cc: 'SG', geo: null },
  { s: '^HSI', n: 'Hang Seng', cc: 'HK', geo: null },
  { s: '^TWII', n: 'Taiwan Weighted', cc: 'TW', geo: 'Taiwan', t12: 1, ll: [127, 22.5] },
  { s: '^KS11', n: 'KOSPI', cc: 'KR', geo: 'South Korea', t12: 1, ll: [132.5, 33.5] },
  { s: '^SET.BK', n: 'SET Composite', cc: 'TH', geo: 'Thailand', t12: 1, ll: [101, 14] },
  { s: '^JKSE', n: 'Jakarta Composite', cc: 'ID', geo: 'Indonesia', t12: 1, ll: [113, -4] },
  { s: '000001.SS', n: 'Shanghai Composite', cc: 'CN', geo: 'China', t12: 1, ll: [96, 33] }];
const INDIA_IDX = [
  { s: '^NSEI', n: 'Nifty 50' }, { s: '^BSESN', n: 'Sensex' },
  { s: '^NSEBANK', n: 'Bank Nifty' }, { s: '^INDIAVIX', n: 'India VIX' }];
const INDIA_SECTORS = [
  { s: '^NSEBANK', n: 'BANK' }, { s: '^CNXIT', n: 'IT' }, { s: '^CNXPHARMA', n: 'PHARMA' },
  { s: '^CNXFMCG', n: 'FMCG' }, { s: '^CNXAUTO', n: 'AUTO' }, { s: '^CNXMETAL', n: 'METAL' },
  { s: '^CNXENERGY', n: 'ENERGY' }, { s: '^CNXREALTY', n: 'REALTY' },
  { s: '^CNXINFRA', n: 'INFRA' }, { s: '^CNXPSUBANK', n: 'PSU BANK' }];
const N50 = [ // [yahoo base, approx index weight]
  ['HDFCBANK',13],['ICICIBANK',9],['RELIANCE',8.5],['INFY',5],['BHARTIARTL',4.5],['TCS',4],
  ['ITC',3.5],['LT',3.5],['AXISBANK',3],['KOTAKBANK',3],['SBIN',3],['M&M',2.5],['BAJFINANCE',2.5],
  ['HINDUNILVR',2],['SUNPHARMA',1.8],['HCLTECH',1.7],['MARUTI',1.6],['NTPC',1.5],['TITAN',1.4],
  ['ULTRACEMCO',1.4],['TATAMOTORS',1.2],['POWERGRID',1.2],['ASIANPAINT',1],['BAJAJFINSV',1],
  ['ADANIPORTS',1],['COALINDIA',1],['NESTLEIND',.9],['ONGC',.9],['BAJAJ-AUTO',.9],['ADANIENT',.8],
  ['WIPRO',.8],['JSWSTEEL',.8],['GRASIM',.8],['HINDALCO',.8],['TATASTEEL',.8],['TECHM',.7],
  ['DRREDDY',.7],['CIPLA',.7],['SBILIFE',.7],['TRENT',.7],['ETERNAL',.7],['HDFCLIFE',.6],
  ['EICHERMOT',.6],['APOLLOHOSP',.6],['TATACONSUM',.6],['BEL',.6],['JIOFIN',.6],['INDUSINDBK',.5],
  ['BRITANNIA',.5],['HEROMOTOCO',.5],['BPCL',.5],['SHRIRAMFIN',.5]];
const NIFTY50 = N50.map(([b, w]) => ({ s: b + '.NS', n: b, w }));
/* Index weights by symbol. The N50 list above is a static snapshot and goes stale
   every time NSE reconstitutes the index (TATAMOTORS, INDUSINDBK, BRITANNIA,
   HEROMOTOCO and BPCL were all dropped and left permanently blank tiles). NSE
   returns the authoritative constituent list on every poll, so we let that drive
   the panel and use this map only for tile sizing. Unknown symbols get a default. */
const N50_W = Object.fromEntries(N50.map(([b, w]) => [b, w]));
let N50_LIVE = [];   // [{n, price, chg}] — authoritative list, refreshed by pollNSEStocks
const MACRO = [{ s: '^VIX', n: 'VIX' }, { s: 'DX-Y.NYB', n: 'DXY' }];
const BONDS = [ /* CBOE yield indices: price/10 = yield% */
  { s: '^IRX', n: 'US 3M', div: 1 }, { s: '^FVX', n: 'US 5Y', div: 1 },
  { s: '^TNX', n: 'US 10Y', div: 1 }, { s: '^TYX', n: 'US 30Y', div: 1 }];
/*
 * Commodity futures — actual spot/front-month prices via Yahoo Finance chart API.
 * The spark API doesn't support =F symbols, so we use a separate pollComs() poller
 * that calls v8/finance/chart per symbol (staggered to avoid rate-limits).
 */
const COMS = [
  /* Precious Metals */
  { s: 'GC=F',  n: 'Gold',        u: '$/oz',    cat: 'PRECIOUS'  },
  { s: 'SI=F',  n: 'Silver',      u: '$/oz',    cat: 'PRECIOUS'  },
  { s: 'PA=F',  n: 'Palladium',   u: '$/oz',    cat: 'PRECIOUS'  },
  { s: 'PL=F',  n: 'Platinum',    u: '$/oz',    cat: 'PRECIOUS'  },
  /* Energy */
  { s: 'CL=F',  n: 'WTI Crude',   u: '$/bbl',   cat: 'ENERGY'    },
  { s: 'BZ=F',  n: 'Brent',       u: '$/bbl',   cat: 'ENERGY'    },
  { s: 'NG=F',  n: 'Nat Gas',     u: '$/MMBtu', cat: 'ENERGY'    },
  { s: 'RB=F',  n: 'Gasoline',    u: '$/gal',   cat: 'ENERGY'    },
  /* Base Metals */
  { s: 'HG=F',  n: 'Copper',      u: '$/lb',    cat: 'METALS'    },
  { s: 'ZN=F',  n: 'Zinc',        u: '¢/lb',    cat: 'METALS'    },
  /* Agriculture */
  { s: 'ZW=F',  n: 'Wheat',       u: 'c/bu',    cat: 'AGRI'      },
  { s: 'ZC=F',  n: 'Corn',        u: 'c/bu',    cat: 'AGRI'      },
  { s: 'ZS=F',  n: 'Soybeans',    u: 'c/bu',    cat: 'AGRI'      },
  { s: 'KC=F',  n: 'Coffee',      u: 'c/lb',    cat: 'AGRI'      },
  { s: 'CT=F',  n: 'Cotton',      u: 'c/lb',    cat: 'AGRI'      },
  { s: 'SB=F',  n: 'Sugar',       u: 'c/lb',    cat: 'AGRI'      },
  /* Thematic ETFs — these work fine with spark API */
  { s: 'URA',   n: 'Uranium',     u: 'ETF',     cat: 'THEMATIC'  },
  { s: 'LIT',   n: 'Lithium',     u: 'ETF',     cat: 'THEMATIC'  },
  { s: 'WOOD',  n: 'Timber',      u: 'ETF',     cat: 'THEMATIC'  },
  { s: 'PDBC',  n: 'Diversified', u: 'ETF',     cat: 'THEMATIC'  },
];
const SECTORS = [ /* sector ETFs as flow/momentum proxies */
  { s: 'AIQ', n: 'AI' }, { s: 'SMH', n: 'SEMIS' }, { s: 'XLK', n: 'TECH' },
  { s: 'ITA', n: 'DEFENCE' }, { s: 'XLF', n: 'BANKING' }, { s: 'XLI', n: 'INDUSTRIALS' },
  { s: 'XLE', n: 'ENERGY' }, { s: 'VOX', n: 'TELECOM' }, { s: 'XLV', n: 'PHARMA' },
  { s: 'XLP', n: 'FMCG' }, { s: 'XLU', n: 'UTILITIES' }];
const FX = [
  { s: 'EURUSD=X', n: 'EUR', full: 'Euro',            pair: 'EUR/USD', inv: false, cc: 'eu' },
  { s: 'GBPUSD=X', n: 'GBP', full: 'British Pound',   pair: 'GBP/USD', inv: false, cc: 'gb' },
  { s: 'JPY=X',    n: 'JPY', full: 'Japanese Yen',     pair: 'USD/JPY', inv: true,  cc: 'jp' },
  { s: 'CHF=X',    n: 'CHF', full: 'Swiss Franc',      pair: 'USD/CHF', inv: true,  cc: 'ch' },
  { s: 'AUDUSD=X', n: 'AUD', full: 'Australian Dollar',pair: 'AUD/USD', inv: false, cc: 'au' },
  { s: 'CADUSD=X', n: 'CAD', full: 'Canadian Dollar',  pair: 'USD/CAD', inv: true,  cc: 'ca' },
  { s: 'INR=X',    n: 'INR', full: 'Indian Rupee',     pair: 'USD/INR', inv: true,  cc: 'in' },
  { s: 'CNY=X',    n: 'CNY', full: 'Chinese Yuan',     pair: 'USD/CNY', inv: true,  cc: 'cn' },
  { s: 'SGD=X',    n: 'SGD', full: 'Singapore Dollar', pair: 'USD/SGD', inv: true,  cc: 'sg' },
  { s: 'HKD=X',    n: 'HKD', full: 'Hong Kong Dollar', pair: 'USD/HKD', inv: true,  cc: 'hk' },
  { s: 'KRW=X',    n: 'KRW', full: 'South Korean Won', pair: 'USD/KRW', inv: true,  cc: 'kr' },
  { s: 'BRL=X',    n: 'BRL', full: 'Brazilian Real',   pair: 'USD/BRL', inv: true,  cc: 'br' },
];
const FAST_YH = [...new Set([...INDICES.filter(x => x.t12).map(x => x.s),
  '^NSEBANK', '^INDIAVIX', '^VIX', 'DX-Y.NYB', '^BSESN', '^NDX'])];
const SPARK_EXCL = new Set(['^CNXGIFTNIFTY']); // spark API can't handle these; pollComs covers them
const ALL_YH = [...new Set([...INDICES, ...MACRO, ...BONDS, ...COMS, ...SECTORS, ...FX,
  ...INDIA_IDX, ...INDIA_SECTORS, ...NIFTY50].map(x => x.s))]
  .filter(s => !SPARK_EXCL.has(s));
const SLOW_YH = ALL_YH.filter(x => !FAST_YH.includes(x));

const COUNTRY_MAP = [ /* treemap: country <- proxy index, capWeight ~ share of world mkt cap */
  { n: 'USA', s: '^GSPC', cap: 48 }, { n: 'INDIA', s: '^NSEI', cap: 4.5 },
  { n: 'CHINA/HK', s: '^HSI', cap: 10 }, { n: 'EUROPE', s: '^GDAXI', cap: 14 },
  { n: 'JAPAN', s: '^N225', cap: 6 }, { n: 'UK', s: '^FTSE', cap: 3.2 },
  { n: 'BRAZIL', s: '^BVSP', cap: 1.5 }];

/* ---------------- 3-year history cache ---------------- */
const HIST_CACHE = new Map(); // sym -> {ts, closes}
const HIST_TTL = 12 * 60 * 60_000; // 12 hours

async function fetchYahooHist(sym, range='3y') {
  const key = sym + '_' + range;
  const cached = HIST_CACHE.get(key);
  if (cached && Date.now() - cached.ts < HIST_TTL) return cached.closes;
  /* 1y→weekly, 3y→weekly, 5y→monthly (keeps point count reasonable) */
  const interval = range === '5y' ? '1mo' : '1wk';
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=${interval}&range=${range}`;
  try {
    const d = await jget(url, { Accept: 'application/json' });
    const raw = d?.chart?.result?.[0]?.indicators?.quote?.[0]?.close || [];
    const closes = raw.map(v => v == null ? null : +v.toFixed(4));
    if (closes.length > 5) { HIST_CACHE.set(key, { ts: Date.now(), closes }); return closes; }
  } catch (e) { console.error('hist fetch', sym, range, e.message); }
  return null;
}

/* ---------------- commodity futures poller (chart API, supports =F symbols) ---------------- */
/*
 * spark API doesn't support =F futures — use v8/finance/chart with range=5d&interval=1d.
 * Daily range always has data regardless of whether market is currently open.
 * meta.regularMarketPrice gives latest; meta.chartPreviousClose gives prior close.
 */
async function pollOneFuture(s) {
  const urls = [
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(s)}?interval=1d&range=5d`,
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(s)}?interval=5m&range=1d`,
  ];
  for (const url of urls) {
    try {
      const d = await jget(url, { Accept: 'application/json' }, 10000);
      const res = d?.chart?.result?.[0];
      if (!res) continue;
      const meta = res.meta || {};
      let price = meta.regularMarketPrice ?? meta.price ?? null;
      let prev  = meta.chartPreviousClose ?? meta.previousClose ?? null;
      if (!price) {
        const closes = (res.indicators?.quote?.[0]?.close || []).filter(x => x != null);
        if (!closes.length) continue;
        price = closes[closes.length - 1];
        if (!prev && closes.length > 1) prev = closes[closes.length - 2];
      }
      if (!price) continue;
      const changePct = prev ? (price / prev - 1) * 100 : 0;
      const spark = (res.indicators?.quote?.[0]?.close || []).filter(x => x != null).slice(-60);
      /* NSE real-time wins on price for Indian indices — it's 5s fresh vs Yahoo's
         delayed feed. Take only the sparkline from the chart API in that case. */
      const prevQ = STATE.quotes[s];
      if (prevQ && prevQ.src === 'NSE_RT' && Date.now() - prevQ.ts < 30_000) {
        prevQ.spark = spark;
        return true;
      }
      STATE.quotes[s] = { price: +price.toFixed(4), changePct: +changePct.toFixed(3),
        spark, ts: Date.now(), src: 'YAHOO_CHART' };
      return true;
    } catch (e) { /* try next url */ }
  }
  return false;
}

// Symbols that need chart API (spark API doesn't support them)
const CHART_API_SYMS = [
  ...COMS.filter(c => c.s.endsWith('=F')).map(c => c.s),
  'ES=F',           // S&P 500 E-mini futures — 23h/day live proxy for F&G when US cash closed
  /* Yahoo's SPARK endpoint refuses these from datacenter IPs (observed on Vercel:
     13/17 fast symbols, ^BSESN always null). The CHART endpoint serves them fine
     (17/17), so route them here instead. Also gives Indian indices real sparklines,
     which pollNSEQuotes then preserves while refreshing the live price. */
  '^BSESN',         // Sensex — BSE, no NSE fallback exists
  '^NSEI', '^NSEBANK', '^INDIAVIX',
];

/* Polled in small parallel batches rather than one-at-a-time. The old sequential
   loop with 250ms gaps took CHART_API_SYMS × (fetch + 250ms) — over 20s once the
   Indian indices were added, which blew past the startup budget on Vercel and left
   the function never becoming ready. Batches of 5 keep Yahoo happy and cut it to ~3s. */
async function pollComs() {
  let ok = 0;
  for (let i = 0; i < CHART_API_SYMS.length; i += 5) {
    const batch = CHART_API_SYMS.slice(i, i + 5);
    const res = await Promise.all(batch.map(s => pollOneFuture(s).catch(() => false)));
    ok += res.filter(Boolean).length;
    if (i + 5 < CHART_API_SYMS.length) await new Promise(r => setTimeout(r, 200));
  }
  setStatus('coms', ok > 0, `${ok}/${CHART_API_SYMS.length} chart-api symbols`);
  if (ok) { derive(); broadcast(); }
}

/* ---------------- poller: Finnhub fallback (free tier: 60 req/min) --------------------
 * Kicks in when Yahoo returns stale (>5 min) or null for a symbol.
 * Requires FINNHUB_API_KEY in .env — no-op when key absent.
 *
 * Symbol mapping: Yahoo tickers differ from Finnhub.
 *   Finnhub indices use  "^" prefix removed, e.g. ^GSPC -> S&P 500 OANDA CF
 *   Futures: GC=F -> GC1! (continuous contract notation on Finnhub)
 *   FX: EURUSD=X -> OANDA:EUR_USD
 *   US stocks/ETFs: XLK -> XLK (same)
 * Coverage: prioritise indices/FX/ETFs; futures are already well-covered by pollOneFuture.
 * ----------------------------------------------------------------------------------*/
const FINNHUB_MAP = {
  // Global indices — Yahoo symbol : Finnhub symbol
  '^DJI':    'OANDA:US30_USD',   '^GSPC':   'OANDA:SPX500_USD',
  '^NDX':    'OANDA:NAS100_USD', '^FTSE':   'OANDA:UK100_GBP',
  '^FCHI':   'OANDA:FR40_EUR',   '^GDAXI':  'OANDA:DE30_EUR',
  '^N225':   'OANDA:JP225_USD',  '^HSI':    'OANDA:HK33_HKD',
  '^KS11':   'OANDA:CHINAH_USD', '^STI':    null, // not available
  '^NSEI':   'OANDA:IN50_USD',   '^BSESN':  null, // no Finnhub mapping
  '^INDIAVIX': null,
  // Macro
  '^VIX':    null,  // not on Finnhub free
  'DX-Y.NYB': 'OANDA:USD_HKD',  // DXY not on Finnhub; skip — deriveDXY handles it
  // Bonds — skip, covered by Treasury poller
  '^TNX': null, '^TYX': null, '^FVX': null, '^IRX': null,
  // FX (Finnhub forex endpoint: /api/v1/forex/rates?base=USD covers all at once)
  'EURUSD=X': null, 'GBPUSD=X': null, 'JPY=X': null, 'CHF=X': null,
  'AUDUSD=X': null, 'CADUSD=X': null, 'INR=X': null, 'CNY=X': null,
  'SGD=X': null, 'HKD=X': null, 'KRW=X': null, 'BRL=X': null,
  // ETF sectors — same symbol on Finnhub
  'AIQ': 'AIQ', 'SMH': 'SMH', 'XLK': 'XLK', 'ITA': 'ITA',
  'XLF': 'XLF', 'XLI': 'XLI', 'XLE': 'XLE', 'VOX': 'VOX',
  'XLV': 'XLV', 'XLP': 'XLP', 'XLU': 'XLU',
  'URA': 'URA', 'LIT': 'LIT', 'WOOD': 'WOOD', 'PDBC': 'PDBC',
};

// FX symbols covered by Finnhub /forex/rates (base=USD, rate = units per USD)
const FINNHUB_FX_SYMBOLS = [
  { yh: 'EURUSD=X', fh: 'EUR', inv: false }, // inv=false → price = 1/rate
  { yh: 'GBPUSD=X', fh: 'GBP', inv: false },
  { yh: 'JPY=X',    fh: 'JPY', inv: true  }, // inv=true  → price = rate (USD per JPY pair = JPY per USD)
  { yh: 'CHF=X',    fh: 'CHF', inv: true  },
  { yh: 'AUDUSD=X', fh: 'AUD', inv: false },
  { yh: 'CADUSD=X', fh: 'CAD', inv: true  },
  { yh: 'INR=X',    fh: 'INR', inv: true  },
  { yh: 'CNY=X',    fh: 'CNY', inv: true  },
  { yh: 'SGD=X',    fh: 'SGD', inv: true  },
  { yh: 'HKD=X',    fh: 'HKD', inv: true  },
  { yh: 'KRW=X',    fh: 'KRW', inv: true  },
  { yh: 'BRL=X',    fh: 'BRL', inv: true  },
];

const STALE_MS = 5 * 60_000; // 5 minutes — threshold to consider a quote stale

function isStale(sym) {
  const q = STATE.quotes[sym];
  return !q || !q.price || (Date.now() - q.ts > STALE_MS);
}

async function pollFinnhubQuote(yhSym, fhSym) {
  try {
    const j = await jget(
      `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(fhSym)}&token=${FINNHUB_KEY}`,
      { 'X-Finnhub-Token': FINNHUB_KEY }, 8000);
    // Finnhub quote: {c: current, pc: previousClose, dp: changePercent, d: change, h, l, o, t}
    if (!j || !j.c || j.c === 0) return false;
    const existing = STATE.quotes[yhSym];
    STATE.quotes[yhSym] = {
      price: +j.c.toFixed(4),
      changePct: j.dp != null ? +j.dp.toFixed(3) : (j.pc ? (j.c / j.pc - 1) * 100 : 0),
      spark: existing?.spark || [],
      ts: Date.now(), src: 'FINNHUB',
    };
    return true;
  } catch { return false; }
}

async function pollFinnhubFX() {
  // Single call fetches all FX rates vs USD
  try {
    const j = await jget(
      `https://finnhub.io/api/v1/forex/rates?base=USD&token=${FINNHUB_KEY}`,
      { 'X-Finnhub-Token': FINNHUB_KEY }, 8000);
    if (!j || !j.quote) return 0;
    let filled = 0;
    for (const { yh, fh, inv } of FINNHUB_FX_SYMBOLS) {
      if (!isStale(yh)) continue;
      const rate = j.quote[fh]; // USD per 1 unit of fh (e.g. EUR→0.92)
      if (!rate) continue;
      // Yahoo convention: inv=false → price = 1/rate (e.g. EURUSD = 1/0.92 ≈ 1.087)
      //                   inv=true  → price = rate (e.g. JPY=X = 155.0 = USD/JPY rate)
      const price = inv ? +rate.toFixed(4) : +(1 / rate).toFixed(6);
      const existing = STATE.quotes[yh];
      const prev = existing?.price;
      STATE.quotes[yh] = {
        price, changePct: prev ? (price / prev - 1) * 100 : 0,
        spark: existing?.spark || [], ts: Date.now(), src: 'FINNHUB_FX',
      };
      filled++;
    }
    return filled;
  } catch { return 0; }
}

async function pollFinnhub() {
  if (!FINNHUB_KEY) return;
  let filled = 0, skipped = 0;

  // 1. FX rates — single API call covers all currencies
  const fxFilled = await pollFinnhubFX();
  filled += fxFilled;

  // 2. Indices / ETFs — only fetch stale symbols
  const toFetch = Object.entries(FINNHUB_MAP)
    .filter(([yh, fh]) => fh && isStale(yh))
    .map(([yh, fh]) => ({ yh, fh }));

  for (let i = 0; i < toFetch.length; i++) {
    const { yh, fh } = toFetch[i];
    const ok = await pollFinnhubQuote(yh, fh);
    if (ok) filled++; else skipped++;
    // Rate-limit: 60 calls/min → 1 per second to be safe
    if (i < toFetch.length - 1) await new Promise(r => setTimeout(r, 1100));
  }

  if (filled > 0 || fxFilled > 0) {
    setStatus('finnhub', true, `${filled} filled (${skipped} failed)`);
    derive(); broadcast();
  } else if (toFetch.length === 0 && fxFilled === 0) {
    setStatus('finnhub', true, 'all symbols fresh — no fallback needed');
  } else {
    setStatus('finnhub', false, `0/${toFetch.length} filled — check key or limits`);
  }
}

/* ---------------- state ---------------- */
/* ---- Correlation routing: destination symbols ---- */
const CORR_SYMS = [
  { t: 'US Tech',        s: 'XLK',  c: 'in'   },
  { t: 'Gold',           s: 'GLD',  c: 'gold'  },
  { t: 'US Treasuries',  s: 'TLT',  c: 'liq'   },
  /* BTC-USD, not IBIT: the ETF only listed Jan-2024, so it cannot match any
     FII-sell week before then regardless of how wide the lookback is. BTC-USD
     has history back to 2014 and is the underlying rather than a wrapper. */
  { t: 'Crypto',         s: 'BTC-USD', c: 'liq'   },
  { t: 'Cash / T-Bills', s: 'BIL',  c: 'out'   },
];

/* Routing mode: persisted to disk so restarts preserve the choice */
const ROUTING_MODE_FILE = path.join(__dirname, 'data', 'routing_mode.json');

let ROUTING_MODE = 'correlation'; // 'correlation' | 'momentum'
try { ROUTING_MODE = JSON.parse(fs.readFileSync(ROUTING_MODE_FILE, 'utf8')).mode || 'correlation'; } catch {}

let CORR_WEIGHTS = null; // {mode, weights, sellWeeks, windowWeeks, computed} | {mode:'fallback', reason}

/* Weekly close series WITH timestamps (for correlation alignment) */
async function fetchHistFull(sym, range = '2y') {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1wk&range=${range}`;
  try {
    const d = await jget(url, { Accept: 'application/json' });
    const res = d?.chart?.result?.[0];
    if (!res) return [];
    const ts = res.timestamp || [];
    const closes = res.indicators?.quote?.[0]?.close || [];
    const out = [];
    for (let i = 1; i < ts.length; i++) {
      if (closes[i] == null || closes[i - 1] == null) continue;
      out.push({ date: new Date(ts[i] * 1000).toISOString().slice(0, 10), ret: (closes[i] / closes[i - 1] - 1) * 100 });
    }
    return out;
  } catch (e) { console.error('[corr hist]', sym, e.message); return []; }
}

function weekKey(dateStr) { /* Monday of the ISO week containing dateStr */
  const d = new Date(dateStr + 'T12:00:00Z');
  const day = d.getUTCDay(); // 0=Sun
  d.setUTCDate(d.getUTCDate() + (day === 0 ? -6 : 1 - day));
  return d.toISOString().slice(0, 10);
}

async function computeCorrelationWeights() {
  /* 1. Aggregate FIIHIST into weekly FII net */
  const weekFII = {};
  for (const r of FIIHIST) {
    if (r.fii == null) continue;
    const wk = weekKey(r.d);
    weekFII[wk] = (weekFII[wk] || 0) + r.fii;
  }
  const fiiWeeks = Object.keys(weekFII).sort();
  if (fiiWeeks.length < 20) {
    CORR_WEIGHTS = { mode: 'fallback', reason: 'insufficient FII history (<20 weeks)' }; return;
  }
  const sellWeeks = fiiWeeks.filter(wk => weekFII[wk] < 0);
  if (sellWeeks.length < 10) {
    CORR_WEIGHTS = { mode: 'fallback', reason: 'too few FII-sell weeks (<10)' }; return;
  }

  /* 2. Fetch weekly return series for each destination symbol.
   *
   * This used to request '2y' while FIIHIST spans ~13 years. Every sell week older
   * than the asset window silently produced null and was dropped by the filter
   * below — 307 of 393 weeks discarded — yet the UI still reported the full 393.
   * Ask for 10y so the windows actually overlap, and fall back to 2y per symbol if
   * a long request comes back thin, so a Yahoo change degrades instead of breaking. */
  const assetMaps = {};
  for (const sym of CORR_SYMS) {
    let hist = await fetchHistFull(sym.s, '10y');
    if (hist.length < 60) {
      const short = await fetchHistFull(sym.s, '2y');
      if (short.length > hist.length) hist = short;
    }
    const map = {};
    for (const { date, ret } of hist) map[date] = ret;
    assetMaps[sym.s] = map;
    await new Promise(r => setTimeout(r, 300)); // gentle on Yahoo
  }

  /* 3. On each FII-sell week, what did each asset do? → avg return.
   *    matchedPerAsset records how many weeks each symbol could actually answer
   *    for, so the reported sample size reflects evidence rather than intent. */
  const weights = {};
  const matchedPerAsset = {};
  for (const sym of CORR_SYMS) {
    const map = assetMaps[sym.s];
    const rets = sellWeeks.map(wk => {
      /* Yahoo week bar date is usually the Friday; scan Mon–Fri of that week */
      const mon = new Date(wk + 'T12:00:00Z');
      for (let d = 0; d <= 6; d++) {
        const probe = new Date(mon); probe.setUTCDate(mon.getUTCDate() + d);
        const ds = probe.toISOString().slice(0, 10);
        if (map[ds] != null) return map[ds];
      }
      return null;
    }).filter(r => r != null);
    matchedPerAsset[sym.t] = rets.length;
    const avg = rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : 0;
    weights[sym.t] = Math.max(0, avg); // only assets that historically RISE when FII sells India
  }

  const wsum = Object.values(weights).reduce((a, b) => a + b, 0);
  if (wsum < 0.01) {
    CORR_WEIGHTS = { mode: 'fallback', reason: 'no positive historical correlations in this window' }; return;
  }

  const normalized = {};
  for (const [k, v] of Object.entries(weights)) normalized[k] = +(v / wsum).toFixed(4);

  /* Report the weeks that actually contributed. Assets have differing listing
     dates, so quote the weakest — the weights are only as good as the thinnest leg. */
  const matchedCounts = Object.values(matchedPerAsset);
  const matchedWeeks = matchedCounts.length ? Math.min(...matchedCounts) : 0;

  CORR_WEIGHTS = { mode: 'correlation', weights: normalized,
    sellWeeks: matchedWeeks,                  // evidence actually used
    sellWeeksFound: sellWeeks.length,         // sell weeks in the FII record
    matchedPerAsset,
    windowWeeks: fiiWeeks.length, computed: Date.now() };
  console.log(`[corr] ${matchedWeeks}/${sellWeeks.length} sell weeks matched →`, JSON.stringify(normalized));
  derive(); broadcast();
}

/* ---------------- FII/DII history store (persists across restarts) ---------------- */
const DATA_DIR = path.join(__dirname, 'data');
const HIST_FILE = path.join(DATA_DIR, 'fii_history.json');
let FIIHIST = []; // [{d:'2026-06-10', fii:-2124.98, dii:3123.95}]
try { FIIHIST = JSON.parse(fs.readFileSync(HIST_FILE, 'utf8')); } catch { FIIHIST = []; }
function saveHist() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(HIST_FILE, JSON.stringify(FIIHIST)); } catch (e) { console.error('hist save:', e.message); }
}
const MON = { jan:1, feb:2, mar:3, apr:4, may:5, jun:6, jul:7, aug:8, sep:9, oct:10, nov:11, dec:12 };
function parseDate(str) { // -> 'YYYY-MM-DD' | null. Accepts 10-Jun-2026, 10 Jun 2026, 10/06/2026, 2026-06-10
  let m = str.match(/(\d{1,2})[\s\-\/]([A-Za-z]{3})[a-z]*[\s\-\/](\d{4})/);
  if (m && MON[m[2].toLowerCase()]) return `${m[3]}-${String(MON[m[2].toLowerCase()]).padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  m = str.match(/(\d{4})-(\d{2})-(\d{2})/); if (m) return m[0];
  m = str.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  return null;
}
function upsertHist(d, fii, dii) {
  if (!d || (fii == null && dii == null)) return false;
  const row = FIIHIST.find(r => r.d === d);
  if (row) { if (fii != null) row.fii = fii; if (dii != null) row.dii = dii; }
  else FIIHIST.push({ d, fii: fii ?? null, dii: dii ?? null });
  return true;
}
function importText(text) { // flexible parser for pasted Moneycontrol/Trendlyne rows
  let n = 0;
  for (const line of text.split(/\n/)) {
    const d = parseDate(line); if (!d) continue;
    const stripped = line
      .replace(/\d{1,2}[\s\-\/][A-Za-z]{3}[a-z]*[\s\-\/,']*\d{2,4}/g, ' ')
      .replace(/\d{4}-\d{2}-\d{2}/g, ' ')
      .replace(/\d{1,2}\/\d{1,2}\/\d{2,4}/g, ' ');
    const vals = (stripped.replace(/(\d),(\d)/g, '$1$2').match(/-?\d+(?:\.\d+)?/g) || [])
      .map(Number).filter(x => Math.abs(x) < 1e5); // cap at ₹1 lakh crore — beyond any real daily flow
    let fii = null, dii = null;
    if (vals.length >= 6) { fii = vals[2]; dii = vals[5]; }       // gross buy/sell/net x2 (Moneycontrol)
    else if (vals.length === 2) { fii = vals[0]; dii = vals[1]; } // net-only rows
    else if (vals.length === 4) { fii = vals[1]; dii = vals[3]; } // buy/net x2 variants
    else continue;
    if (upsertHist(d, fii, dii)) n++;
  }
  if (n) { FIIHIST.sort((a, b) => a.d < b.d ? -1 : 1); saveHist(); }
  return n;
}
// seed with the one verified real datapoint (NSE provisional, 10-Jun-2026)
if (!FIIHIST.length) { upsertHist('2026-06-10', -2124.98, 3123.95); saveHist(); }

const STATE = {
  startedAt: Date.now(),
  quotes: {},          // sym -> {price, changePct, spark[], ts}
  crypto: {},          // id  -> {usd, chg24h, mcap, ts}
  cgGlobal: null,      // total mcap, btc dominance
  fii: { data: null, error: null, ts: 0 },
  se: null,
  fred: { data: {}, enabled: !!FRED_KEY, error: null, ts: 0 },
  treasury: null,
  derived: {},
  pollerStatus: {},
};

/* ---------------- helpers ---------------- */
async function jget(url, headers = {}, timeout = 12000) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeout);
  try {
    const r = await fetch(url, { headers: { ...UA, ...headers }, signal: ctl.signal });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  } finally { clearTimeout(t); }
}
async function tget(url, headers = {}, timeout = 12000) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeout);
  try {
    const r = await fetch(url, { headers: { ...UA, ...headers }, signal: ctl.signal });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.text();
  } finally { clearTimeout(t); }
}

/* ---------------- RSS/Atom parser (no deps, regex-based) ---------------- */
function parseRSS(xml, max = 12) {
  const items = [];
  // Support both RSS <item> and Atom <entry>
  const rx = /<(?:item|entry)>([\s\S]*?)<\/(?:item|entry)>/g;
  let m;
  while ((m = rx.exec(xml)) !== null && items.length < max) {
    const b = m[1];
    const get = tag => {
      // Handles CDATA, regular text, and attributes on the tag
      const r = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tag}>`, 'i');
      const found = r.exec(b);
      if (!found) return '';
      // Strip CDATA wrappers
      return found[1].replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '').trim();
    };
    // For Atom <link href="..."/>
    const getLinkAttr = () => {
      const r = /<link[^>]+href=["']([^"']+)["']/i;
      const found = r.exec(b);
      return found ? found[1] : '';
    };
    const dec = s => s
      .replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>')
      .replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&#8230;/g,'…')
      .replace(/&#8216;/g,'‘').replace(/&#8217;/g,'’')
      .replace(/&#\d+;/g, c => { try { return String.fromCharCode(parseInt(c.slice(2,-1))); } catch { return c; } });
    const title = dec(get('title'));
    if (!title) continue;
    const rawLink = (get('link') || getLinkAttr() || get('guid') || get('id')).replace(/\s+/g,'');
    const desc = dec(get('description') || get('summary') || get('content')).replace(/<[^>]+>/g,'').trim().slice(0, 200);
    const pubDate = get('pubDate') || get('published') || get('updated');
    items.push({ title, link: rawLink || '#', desc, pubDate });
  }
  return items;
}

/* ---------------- poller: news (RSS feeds, cached 15 min) ---------------- */
const NEWS_CACHE = { global: [], india: [], ts: 0, log: [] };
const NEWS_TTL = 15 * 60_000;

async function pollNews() {
  // Primary: Google News RSS (free, no auth, reliable)
  // Fallback: direct publisher RSS
  const feeds = [
    {
      key: 'global',
      urls: [
        'https://www.cnbc.com/id/100003114/device/rss/rss.html',              // CNBC Markets
        'https://feeds.bbci.co.uk/news/business/rss.xml',                     // BBC Business
        'https://rss.nytimes.com/services/xml/rss/nyt/Business.xml',          // NYT Business
        'https://finance.yahoo.com/rss/topstories',                           // Yahoo Finance
      ]
    },
    {
      key: 'india',
      urls: [
        'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms', // ET Markets
        'https://www.moneycontrol.com/rss/MCtopnews.xml',                       // Moneycontrol
        'https://news.google.com/rss/search?q=nifty+sensex+NSE+market&hl=en-IN&gl=IN&ceid=IN:en',
      ]
    },
  ];
  let ok = 0;
  for (const f of feeds) {
    for (const url of f.urls) {
      try {
        const xml = await tget(url, { Accept: 'application/rss+xml,application/xml,text/xml,*/*' }, 12000);
        const parsed = parseRSS(xml, 12);
        console.log(`[news:${f.key}] ${url} → ${parsed.length} items`);
        if (parsed.length) { NEWS_CACHE[f.key] = parsed; ok++; break; }
      } catch(e) { console.error(`[news:${f.key}] ${url}`, e.message); }
    }
  }
  NEWS_CACHE.ts = Date.now();
  setStatus('news', ok > 0, `${NEWS_CACHE.global.length}G + ${NEWS_CACHE.india.length}I items`);
}
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const last = a => a && a.length ? a[a.length - 1] : null;
function setStatus(name, ok, msg) {
  STATE.pollerStatus[name] = { ok, msg: msg || '', ts: Date.now() };
  if (!ok) console.error(`[${name}] ${msg}`);
}

/* ---------------- poller: Yahoo spark (batch) ---------------- */
async function pollYahoo(symbolSet) {
  const syms = symbolSet || ALL_YH;
  const chunks = [];
  for (let i = 0; i < syms.length; i += 12) chunks.push(syms.slice(i, i + 12));
  let okCount = 0;
  for (const ch of chunks) {
    try {
      const u = `https://query1.finance.yahoo.com/v8/finance/spark?symbols=${encodeURIComponent(ch.join(','))}&range=1d&interval=5m`;
      const j = await jget(u);
      for (const sym of ch) {
        const d = j[sym] || (j.spark && j.spark.result || []).find(r => r.symbol === sym);
        let closes, prev;
        if (d && d.close) { closes = d.close; prev = d.previousClose ?? d.chartPreviousClose; }
        else if (d && d.response && d.response[0]) {
          const r0 = d.response[0];
          closes = r0.indicators.quote[0].close;
          prev = r0.meta.previousClose ?? r0.meta.chartPreviousClose;
        }
        if (!closes) continue;
        const cl = closes.filter(x => x != null);
        if (!cl.length) continue;
        const price = last(cl);
        const base = prev ?? cl[0];
        /* NSE real-time is fresher and undelayed for Indian symbols. Keep Yahoo as the
           fallback (it's the only source if NSE is unreachable) but don't let a delayed
           quote overwrite a live one — take just the sparkline in that case. */
        const prevQ = STATE.quotes[sym];
        if (prevQ && prevQ.src === 'NSE_RT' && Date.now() - prevQ.ts < 60_000) {
          prevQ.spark = cl.slice(-60);
          okCount++;
          continue;
        }
        STATE.quotes[sym] = {
          price, changePct: base ? (price / base - 1) * 100 : 0,
          spark: cl.slice(-60), ts: Date.now(), src: 'YAHOO',
        };
        okCount++;
      }
      await new Promise(r => setTimeout(r, 250));
    } catch (e) { setStatus('yahoo', false, e.message); }
  }
  if (okCount) setStatus('yahoo', true, `${okCount}/${syms.length} symbols`);
}

/* ---------------- poller: CoinGecko ------------------------------------------------
 * The free tier is a shared ~30 req/min pool and was returning HTTP 429 in
 * production. Three things were wrong:
 *   1. a 35s interval, and each pass made TWO requests (price + global) — roughly
 *      4,900 calls/day
 *   2. /global barely moves (total mcap, BTC dominance) yet was fetched as often
 *      as spot prices
 *   3. a 429 produced no backoff at all, so it kept hammering at the same rate
 *
 * Now: 120s interval (see scheduler), /global only every 5th pass, and exponential
 * backoff on 429 up to 15 minutes. Existing values stay on screen while backed off
 * rather than blanking out.
 * -----------------------------------------------------------------------------------*/
let CG_BACKOFF_UNTIL = 0;
let CG_BACKOFF_MS = 0;
let CG_PASS = 0;

async function pollCG() {
  if (Date.now() < CG_BACKOFF_UNTIL) {
    setStatus('coingecko', false, `rate-limited — retry in ${Math.ceil((CG_BACKOFF_UNTIL - Date.now()) / 1000)}s`);
    return;
  }
  try {
    const j = await jget('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,tether&vs_currencies=usd&include_24hr_change=true&include_market_cap=true');
    for (const [id, v] of Object.entries(j))
      STATE.crypto[id] = { usd: v.usd, chg24h: v.usd_24h_change || 0, mcap: v.usd_market_cap || 0, ts: Date.now() };

    /* Total market cap and BTC dominance move slowly — no need to refetch every pass */
    if (CG_PASS % 5 === 0 || !STATE.cgGlobal) {
      const g = await jget('https://api.coingecko.com/api/v3/global');
      STATE.cgGlobal = {
        mcapChg24h: g.data.market_cap_change_percentage_24h_usd,
        btcDom: g.data.market_cap_percentage?.btc, ts: Date.now(),
      };
    }
    CG_PASS++;
    CG_BACKOFF_MS = 0;                      // recovered — reset the ladder
    setStatus('coingecko', true, 'ok');
  } catch (e) {
    if (/429/.test(e.message)) {
      CG_BACKOFF_MS = CG_BACKOFF_MS ? Math.min(CG_BACKOFF_MS * 2, 15 * 60_000) : 60_000;
      CG_BACKOFF_UNTIL = Date.now() + CG_BACKOFF_MS;
      setStatus('coingecko', false, `HTTP 429 — backing off ${Math.round(CG_BACKOFF_MS / 1000)}s`);
    } else {
      setStatus('coingecko', false, e.message);
    }
  }
}

/* ---------------- poller: NSE FII/DII (T+1, India-IP friendly) ---------------- */
async function pollNSE() {
  try {
    // NSE requires a session cookie from the homepage — cached 10 min by nseCookies()
    const cookies = await nseCookies();
    const j = await jget('https://www.nseindia.com/api/fiidiiTradeReact',
      { Cookie: cookies, Referer: 'https://www.nseindia.com/reports/fii-dii', Accept: 'application/json' });
    STATE.fii = { data: j, error: null, ts: Date.now() };
    try {
      let touched = false;
      for (const r of j) {
        const d = parseDate(r.date || ''); if (!d) continue;
        const net = parseFloat(String(r.netValue).replace(/,/g, ''));
        if (/FII|FPI/i.test(r.category)) touched = upsertHist(d, net, null) || touched;
        else if (/DII/i.test(r.category)) touched = upsertHist(d, null, net) || touched;
      }
      if (touched) { FIIHIST.sort((a, b) => a.d < b.d ? -1 : 1); saveHist(); }
    } catch (e) { console.error('hist upsert:', e.message); }
    setStatus('nse', true, 'FII/DII updated');
  } catch (e) {
    NSE_COOKIES = { v: '', ts: 0 };
    STATE.fii.error = 'NSE unreachable right now — StockEdge fallback covers FII/DII history.';
    setStatus('nse', false, e.message);
  }
}

/* ---------------- poller: StockEdge FII/DII (cash, provisional — same NSE numbers, history-capable) ---------------- */
const SE_URL = 'https://api.stockedge.com/Api/DailyDashboardApi/GetDailyFIIDIIActivities?lang=en';
const SE_BACKFILL_DAYS = +(process.env.SE_BACKFILL_DAYS || 4600); // back to Jan-2014
const META_FILE = path.join(DATA_DIR, 'fii_meta.json');
let FIIMETA = { empty: [] }; // dates known to have no data (holidays) — never re-fetched
try { FIIMETA = JSON.parse(fs.readFileSync(META_FILE, 'utf8')); } catch {}
const EMPTYSET = new Set(FIIMETA.empty || []);
function saveMeta() { try { fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(META_FILE, JSON.stringify({ empty: [...EMPTYSET] })); } catch {} }
async function fetchSE(dateStr) { // dateStr 'YYYY-MM-DD' or null for latest -> {d,fii,dii}|null
  const j = await jget(SE_URL + (dateStr ? `&date=${dateStr}` : ''), { Referer: 'https://web.stockedge.com/' });
  if (!j || !j.Date || !Array.isArray(j.FIIDIIData)) return null;
  const d = j.Date.slice(0, 10);
  const find = re => j.FIIDIIData.find(x => re.test(x.Name || ''));
  const fii = find(/FII Cash Market/i), dii = find(/DII Cash Market/i);
  if (!fii && !dii) return null;
  return { d, fii: fii ? +fii.Value : null, dii: dii ? +dii.Value : null };
}
async function pollSE() {
  try {
    const r = await fetchSE(null);
    if (r) {
      STATE.se = { latest: r, ts: Date.now() };
      if (upsertHist(r.d, r.fii, r.dii)) { FIIHIST.sort((a, b) => a.d < b.d ? -1 : 1); saveHist(); }
      setStatus('stockedge', true, `latest ${r.d}`);
    }
  } catch (e) { setStatus('stockedge', false, e.message); }
}
let SE_BF = { running: false, done: 0, added: 0 };
async function backfillSE() {
  if (SE_BF.running) return;
  /* Skip the sweep entirely when history already reaches the last trading day.
     On a serverless host every cold start was walking ~3,300 dates to build a todo
     list that is almost always empty. Only scan when there is a real gap. */
  const newest = FIIHIST.length ? FIIHIST[FIIHIST.length - 1].d : null;
  if (newest) {
    const ageDays = (Date.now() - new Date(newest + 'T12:00:00Z')) / 864e5;
    if (ageDays < 4) {        // covers a long weekend / market holiday
      setStatus('stockedge-backfill', true, `up to date (${FIIHIST.length} sessions, latest ${newest})`);
      return;
    }
  }
  SE_BF.running = true;
  const have = new Set(FIIHIST.map(r => r.d));
  const todo = [];
  for (let i = 1; i <= SE_BACKFILL_DAYS; i++) {
    const dt = new Date(Date.now() - i * 864e5);
    const wd = dt.getUTCDay(); if (wd === 0 || wd === 6) continue;
    const ds = dt.toISOString().slice(0, 10);
    if (!have.has(ds) && !EMPTYSET.has(ds)) todo.push(ds);
  }
  for (let i = 0; i < todo.length; i += 4) {           // 4 parallel, ~8 req/s burst, once
    await Promise.all(todo.slice(i, i + 4).map(async ds => {
      try {
        const r = await fetchSE(ds);
        SE_BF.done++;
        if (r && r.d === ds && upsertHist(r.d, r.fii, r.dii)) SE_BF.added++;
        else EMPTYSET.add(ds);
      } catch (e) { /* transient — retry next startup */ }
    }));
    if (SE_BF.done % 24 < 4) { FIIHIST.sort((a, b) => a.d < b.d ? -1 : 1); saveHist(); saveMeta(); }
    await new Promise(r => setTimeout(r, 450));
  }
  saveMeta();
  FIIHIST.sort((a, b) => a.d < b.d ? -1 : 1); saveHist();
  SE_BF.running = false;
  setStatus('stockedge-backfill', true, `added ${SE_BF.added} sessions, total ${FIIHIST.length}`);
  console.log(`[backfill] complete: +${SE_BF.added} sessions, ${FIIHIST.length} total`);
}

/* ---------------- US Treasury 10Y (official daily, from FRED) ------------------------
 * Previously hit fiscaldata.treasury.gov/…/avg_interest_rates, which now returns 404
 * and — more importantly — reports the *average coupon on outstanding debt*, not the
 * market yield. Those are different numbers. FRED's DGS10 is the actual daily
 * constant-maturity market yield and is already fetched by pollFRED, so we read it
 * from there instead of making a separate (broken) network call.
 * Yahoo's ^TNX remains the fallback when no FRED key is configured.
 * -----------------------------------------------------------------------------------*/
function pollTreasury() {
  const dgs10 = STATE.fred.data.DGS10;
  if (!dgs10 || !dgs10.length) {
    setStatus('treasury', false, FRED_KEY ? 'DGS10 not loaded yet' : 'needs FRED_API_KEY — using Yahoo ^TNX');
    return;
  }
  const { d, v } = dgs10[0];
  STATE.treasury = { us10y: v, date: d, ts: Date.now() };
  setStatus('treasury', true, `US10Y ${v}% as of ${d} (FRED DGS10)`);
}

/* ---------------- poller: NSE real-time quotes (no delay for Indian markets) ---------------- */
async function pollNSEQuotes() {
  try {
    /* Cookie is cached for 10 min by nseCookies(). Re-handshaking the homepage on
       every 5s tick was ~720 requests/hour and a fast route to an Akamai block. */
    const cookies = await nseCookies();
    const j = await jget('https://www.nseindia.com/api/allIndices',
      { Cookie: cookies, Referer: 'https://www.nseindia.com/', Accept: 'application/json' });
    if (j && j.data && Array.isArray(j.data)) {
      const symMap = { 'NIFTY 50': '^NSEI', 'NIFTY BANK': '^NSEBANK', 'INDIA VIX': '^INDIAVIX',
        'NIFTY IT': '^CNXIT', 'NIFTY AUTO': '^CNXAUTO', 'NIFTY PHARMA': '^CNXPHARMA',
        'NIFTY FMCG': '^CNXFMCG', 'NIFTY METAL': '^CNXMETAL', 'NIFTY ENERGY': '^CNXENERGY',
        'NIFTY REALTY': '^CNXREALTY', 'NIFTY INFRA': '^CNXINFRA', 'NIFTY PSU BANK': '^CNXPSUBANK',
        'GIFT NIFTY': '^CNXGIFTNIFTY' };
      let updated = 0;
      for (const row of j.data) {
        const sym = symMap[row.index];
        if (!sym) continue;
        const price = parseFloat(row.last || row.lastPrice || '0');
        const prev = parseFloat(row.previousClose || row.prev || '0');
        if (!price || !prev) continue;
        const existing = STATE.quotes[sym];
        STATE.quotes[sym] = {
          price, changePct: ((price - prev) / prev) * 100,
          spark: existing?.spark || [], ts: Date.now(), src: 'NSE_RT'
        };
        updated++;
      }
      if (updated) setStatus('nse-quotes', true, `${updated} indices real-time`);
    }
  } catch (e) { NSE_COOKIES = { v: '', ts: 0 }; setStatus('nse-quotes', false, e.message); }
}

/* ---------------- poller: NSE Nifty-50 constituents ---------------------------------
 * Yahoo's spark endpoint returns null for every ".NS" symbol when called from a
 * datacenter IP, which left the entire Nifty 50 heatmap blank on Vercel. NSE's own
 * equity-stockIndices endpoint is not blocked and is real-time rather than delayed,
 * so we source the constituents straight from it and write them into STATE.quotes
 * under the same "<SYMBOL>.NS" keys the rest of the app already expects.
 * -----------------------------------------------------------------------------------*/
let NSE_COOKIES = { v: '', ts: 0 };
async function nseCookies() {
  if (NSE_COOKIES.v && Date.now() - NSE_COOKIES.ts < 10 * 60_000) return NSE_COOKIES.v;
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), 12000);
  try {
    const home = await fetch('https://www.nseindia.com/', {
      headers: { ...UA, 'Accept-Language': 'en-US,en;q=0.9' }, signal: ctl.signal });
    const v = (home.headers.getSetCookie ? home.headers.getSetCookie() : [])
      .map(c => c.split(';')[0]).join('; ');
    NSE_COOKIES = { v, ts: Date.now() };
    return v;
  } finally { clearTimeout(t); }
}

async function pollNSEStocks() {
  try {
    const cookies = await nseCookies();
    /* NSE retired /api/equity-stockIndices (it now 404s) and moved the live market
       watch to this NextApi route. Payload is { data: { data: [...] } } with 51 rows:
       row 0 is the index itself (series === null), rows 1-50 are the constituents.
       pChange is supplied directly, so no need to recompute from previousClose. */
    const j = await jget('https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi?functionName=getIndicesData&symbol=NIFTY%2050',
      { Cookie: cookies, Referer: 'https://www.nseindia.com/market-data/live-equity-market',
        Accept: 'application/json' });
    const rows = j?.data?.data;
    if (!Array.isArray(rows)) { setStatus('nse-stocks', false, 'unexpected payload shape'); return; }
    let updated = 0;
    const live = [];
    for (const row of rows) {
      if (!row.series) continue;                       // index row, not a constituent
      const base = (row.symbol || '').trim();
      if (!base) continue;
      const price = parseFloat(row.lastPrice);
      if (!isFinite(price)) continue;
      let chg = parseFloat(row.pChange);
      if (!isFinite(chg)) {
        const prev = parseFloat(row.previousClose);
        if (!isFinite(prev) || !prev) continue;
        chg = ((price - prev) / prev) * 100;
      }
      const sym = base + '.NS';
      const existing = STATE.quotes[sym];
      STATE.quotes[sym] = {
        price, changePct: chg,
        spark: existing?.spark || [], ts: Date.now(), src: 'NSE_RT',
      };
      live.push({ n: base, price, chg });
      updated++;
    }
    if (live.length) N50_LIVE = live;
    setStatus('nse-stocks', updated > 0, `${updated}/${rows.length - 1} constituents real-time`);
    if (updated) { derive(); broadcast(); }
  } catch (e) { NSE_COOKIES = { v: '', ts: 0 }; setStatus('nse-stocks', false, e.message); }
}

/* ---------------- DXY derived from FX basket (fallback if Yahoo stale) ---------------- */
/* DXY = 50.14348112 * EUR^-0.576 * JPY^0.136 * GBP^-0.119 * CAD^-0.091 * SEK^-0.042 * CHF^-0.036 */
function deriveDXY() {
  try {
    const eur = STATE.quotes['EURUSD=X']?.price;
    const jpy = STATE.quotes['JPY=X']?.price;   // USD per JPY (inverted)
    const gbp = STATE.quotes['GBPUSD=X']?.price;
    const chf = STATE.quotes['CHF=X']?.price;   // USD per CHF (inverted)
    if (!eur || !jpy || !gbp) return;
    // jpy quote is USDJPY, chf is USDCHF
    const dxy = 50.14348112 * Math.pow(eur, -0.576) * Math.pow(jpy, 0.136)
      * Math.pow(gbp, -0.119) * Math.pow(chf || 0.9, -0.036);
    const existing = STATE.quotes['DX-Y.NYB'];
    if (!existing || existing.src !== 'YAHOO' || Date.now() - existing.ts > 60000) {
      // Only override if Yahoo data is stale or missing
      const prev = existing?.price || dxy;
      STATE.quotes['DX-Y.NYB'] = {
        price: +dxy.toFixed(3), changePct: ((dxy - prev) / prev) * 100,
        spark: existing?.spark || [], ts: Date.now(), src: 'FX_DERIVED'
      };
    }
  } catch (e) { /* silent — best-effort */ }
}

/* ---------------- poller: FRED (liquidity components) ---------------- */
const FRED_SERIES = {
  WALCL: 'Fed balance sheet', M2SL: 'US M2', BAMLH0A0HYM2: 'HY OAS spread',
  DGS2: 'US 2Y', DGS10: 'US 10Y', DGS30: 'US 30Y',
};
async function pollFRED() {
  if (!FRED_KEY) { setStatus('fred', false, 'no FRED_API_KEY — HY OAS credit factor and 2s10s curve unavailable'); return; }
  try {
    for (const id of Object.keys(FRED_SERIES)) {
      const j = await jget(`https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${FRED_KEY}&file_type=json&sort_order=desc&limit=60`);
      const obs = j.observations.filter(o => o.value !== '.').map(o => ({ d: o.date, v: +o.value }));
      STATE.fred.data[id] = obs; // newest first
      await new Promise(r => setTimeout(r, 250));
    }
    STATE.fred.ts = Date.now(); STATE.fred.error = null;
    setStatus('fred', true, 'ok');
  } catch (e) { STATE.fred.error = e.message; setStatus('fred', false, e.message); }
}

/* ---------------- trailing history for relative sentiment scoring -------------------
 * Fear & Greed was scored against FIXED bands: VIX on a 10–45 scale, HY OAS on 2–6%.
 * That measures "are levels comfortable in absolute terms", not sentiment. It read
 * 69 (greed) on a day CNN's index read 28 (fear) — because VIX at 16 looks calm on an
 * absolute scale while sitting ABOVE its 50-day average, which is the stressed signal.
 *
 * Sentiment indices are conventionally relative: each input is compared to its own
 * recent history. These helpers fetch the daily closes needed for that, cached for
 * six hours since moving averages barely move intraday.
 * -----------------------------------------------------------------------------------*/
const DAILY_CACHE = new Map();          // sym|range -> {ts, closes[]}
const DAILY_TTL = 6 * 60 * 60_000;

async function fetchDaily(sym, range = '9mo') {
  const key = sym + '|' + range;
  const c = DAILY_CACHE.get(key);
  if (c && Date.now() - c.ts < DAILY_TTL) return c.closes;
  try {
    const d = await jget(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=${range}`,
      { Accept: 'application/json' }, 10000);
    const closes = (d?.chart?.result?.[0]?.indicators?.quote?.[0]?.close || []).filter(x => x != null);
    if (closes.length > 20) { DAILY_CACHE.set(key, { ts: Date.now(), closes }); return closes; }
  } catch (e) { /* leave whatever was cached */ }
  return c ? c.closes : null;
}

const sma = (a, n) => (!a || a.length < n) ? null : a.slice(-n).reduce((x, y) => x + y, 0) / n;
const retN = (a, n) => (!a || a.length < n + 1) ? null : (a[a.length - 1] / a[a.length - 1 - n] - 1) * 100;

/* Indices used for breadth — compared to their OWN 20d average rather than counting
   today's sign, so a market that closed hours ago is treated consistently with one
   still trading. The old count-today's-gainers breadth mixed sessions. */
const BREADTH_SYMS = INDICES.filter(x => x.t12).map(x => x.s);

async function pollFGHistory() {
  try {
    const [vixH, spxH, tltH] = await Promise.all([
      fetchDaily('^VIX', '6mo'), fetchDaily('^GSPC', '12mo'), fetchDaily('TLT', '3mo'),
    ]);
    let above = 0, counted = 0;
    for (const s of BREADTH_SYMS) {
      const h = await fetchDaily(s, '3mo');
      const ma = sma(h, 20);
      if (ma == null) continue;
      counted++;
      if (h[h.length - 1] > ma) above++;
    }
    STATE.fgHist = {
      vixMA50:  sma(vixH, 50),
      spxMA125: sma(spxH, 125),
      spx20d:   retN(spxH, 20),
      tlt20d:   retN(tltH, 20),
      breadthAboveMA: counted ? (above / counted) * 100 : null,
      breadthCounted: counted,
      ts: Date.now(),
    };
    setStatus('fg-history', true,
      `VIX MA50 ${STATE.fgHist.vixMA50?.toFixed(1)}, SPX MA125 ${STATE.fgHist.spxMA125?.toFixed(0)}, breadth ${above}/${counted}`);
  } catch (e) { setStatus('fg-history', false, e.message); }
}

/* ---------------- derived analytics (all tagged DERIVED) ---------------- */
function q(sym) { return STATE.quotes[sym] || null; }
function derive() {
  const d = {};

  /* Fear & Greed: real inputs, simple transparent scoring */
  // Use US Treasury official yield if available and fresher than Yahoo
  const tnxYahoo = q('^TNX');
  const tnxTreasury = STATE.treasury;
  const tnx = (tnxTreasury && (!tnxYahoo || tnxTreasury.ts > tnxYahoo.ts - 3600000))
    ? { price: tnxTreasury.us10y, changePct: tnxYahoo?.changePct ?? 0 }
    : tnxYahoo;
  deriveDXY(); // update DXY from FX basket if Yahoo data stale
  const vix = q('^VIX'), dxy = q('DX-Y.NYB');
  /* When US cash market is closed (overnight for India), use S&P 500 futures (ES=F)
     as a live proxy for both the VIX substitute and yield-change substitute.
     ES=F trades 23/6 so it reflects live global risk sentiment around the clock. */
  const esFut = q('ES=F');  // S&P 500 E-mini futures — 23h/day
  const spyEtf = q('SPY');  // SPY also trades pre/post market
  // US cash session: 09:30–16:00 ET = 19:00–00:30 IST (next day)
  const nowUtcH = new Date().getUTCHours();
  const usCashOpen = nowUtcH >= 13 && nowUtcH < 21; // 09:30–16:00 ET in UTC
  /* ══ FEAR & GREED — scored RELATIVE to trailing history ═══════════════════════════
   * Every factor is now measured against its own recent average rather than a fixed
   * band. The previous absolute scoring produced 69 (greed) on a day CNN's index read
   * 28 (fear): VIX at 16 scores 89 on a 10–45 band while simultaneously sitting ABOVE
   * its 50-day average, which is the stressed reading. Levels and sentiment are not
   * the same quantity, and this index claims to measure the second.
   *
   * Five factors, each 0–100, 50 = neutral (in line with its own recent norm):
   *   volatility  VIX vs 50d average          — above average = fear
   *   momentum    S&P 500 vs 125d average     — the trend input that was missing
   *   credit      HY OAS vs 20d average       — widening = fear
   *   safeHaven   S&P 20d return − TLT 20d    — stocks beating bonds = greed
   *   breadth     % of world indices > own 20d average
   *
   * Breadth compares each index to its OWN average instead of counting today's
   * gainers, so a market that closed hours ago is treated consistently with one still
   * trading. The old version counted Asia's close against the US mid-session.
   *
   * Falls back to the absolute formulation per-factor when history is unavailable,
   * and reports which basis each factor used. */
  const H = STATE.fgHist || {};
  const hy = STATE.fred.data.BAMLH0A0HYM2;
  const basis = {};
  /* Same-day index moves. Still needed by the asset-rotation block below, and used
     as the breadth fallback when trailing history has not loaded. */
  const idxChgs = INDICES.map(i => q(i.s)?.changePct).filter(x => x != null);

  // 1. Volatility — VIX relative to its 50-day average
  let volScore;
  if (vix && H.vixMA50) {
    const rel = vix.price / H.vixMA50 - 1;           // +0.20 = 20% above average
    volScore = clamp(50 - rel * 250, 0, 100);        // ±20% spans the full range
    basis.volatility = `VIX ${vix.price.toFixed(1)} vs 50d avg ${H.vixMA50.toFixed(1)}`;
  } else if (vix) {
    volScore = clamp((45 - vix.price) / 35 * 100, 0, 100);
    basis.volatility = 'VIX absolute band (history unavailable)';
  } else { volScore = 50; basis.volatility = 'unavailable'; }

  // 2. Momentum — S&P 500 versus its 125-day average. Previously absent entirely,
  //    which is why a sustained drawdown left the index reading greed.
  let momScore;
  const spxNow = q('^GSPC')?.price;
  if (spxNow && H.spxMA125) {
    const rel = spxNow / H.spxMA125 - 1;
    momScore = clamp(50 + rel * 500, 0, 100);        // ±10% spans the full range
    basis.momentum = `S&P ${Math.round(spxNow)} vs 125d avg ${Math.round(H.spxMA125)}`;
  } else { momScore = 50; basis.momentum = 'unavailable'; }

  // 3. Credit — HY OAS versus its own 20-day average, not a fixed 2–6% band
  let creditScore;
  const creditLive = !!(hy && hy.length);
  if (creditLive && hy.length >= 20) {
    const oas = hy[0].v;
    const ma20 = hy.slice(0, 20).reduce((a, b) => a + b.v, 0) / 20;
    creditScore = clamp(50 - (oas / ma20 - 1) * 400, 0, 100);
    basis.credit = `HY OAS ${oas.toFixed(2)}% vs 20d avg ${ma20.toFixed(2)}%`;
  } else if (creditLive) {
    creditScore = clamp((6 - hy[0].v) / 4 * 100, 0, 100);
    basis.credit = 'HY OAS absolute band (short history)';
  } else { creditScore = 50; basis.credit = 'PLACEHOLDER — FRED not loaded'; }

  // 4. Safe-haven demand — stocks versus bonds over 20 days
  let havenScore;
  if (H.spx20d != null && H.tlt20d != null) {
    havenScore = clamp(50 + (H.spx20d - H.tlt20d) * 5, 0, 100);
    basis.safeHaven = `S&P 20d ${H.spx20d.toFixed(1)}% vs TLT ${H.tlt20d.toFixed(1)}%`;
  } else { havenScore = 50; basis.safeHaven = 'unavailable'; }

  // 5. Breadth — share of world indices above their own 20-day average
  let breadthScore;
  if (H.breadthAboveMA != null) {
    breadthScore = H.breadthAboveMA;
    basis.breadth = `${Math.round(H.breadthAboveMA)}% of ${H.breadthCounted} indices above 20d avg`;
  } else {
    breadthScore = idxChgs.length ? idxChgs.filter(x => x > 0).length / idxChgs.length * 100 : 50;
    basis.breadth = 'same-day gainers (history unavailable)';
  }

  const f = {
    volatility: volScore,
    momentum:   momScore,
    credit:     creditScore,
    safeHaven:  havenScore,
    breadth:    breadthScore,
  };
  const relativeCount = Object.values(basis).filter(b => !/unavailable|PLACEHOLDER|absolute|same-day/.test(b)).length;
  const fgMode = relativeCount >= 4 ? 'RELATIVE' : relativeCount >= 2 ? 'PARTIAL' : 'DEGRADED';
  const vals = Object.values(f);
  d.fearGreed = {
    score: Math.round(vals.reduce((a, b) => a + b, 0) / vals.length),
    factors: f, mode: fgMode, basis, creditLive,
    relativeFactors: relativeCount,
    inputs: {
      vix: vix?.price, vixMA50: H.vixMA50 ?? null,
      spx: spxNow ?? null, spxMA125: H.spxMA125 ?? null,
      hyOAS: hy?.[0]?.v ?? null,
      spx20d: H.spx20d ?? null, tlt20d: H.tlt20d ?? null,
      us10y: tnx ? tnx.price : null, dxy: dxy?.price, usCashOpen,
    },
  };

  /* Country flow proxies: index move x cap weight (NOT real flows) */
  d.countries = COUNTRY_MAP.map(c => {
    const qq = q(c.s);
    return { n: c.n, cap: c.cap, chgPct: qq?.changePct ?? null,
      flowProxy: qq ? +(qq.changePct * c.cap / 10).toFixed(2) : null };
  });

  /* Sector radar: ETF day% -> momentum rank + flow proxy */
  const secs = SECTORS.map(s => ({ n: s.n, chg: q(s.s)?.changePct ?? null }))
    .filter(s => s.chg != null).sort((a, b) => b.chg - a.chg);
  d.sectors = secs.map((s, i) => ({ n: s.n, chgPct: +s.chg.toFixed(2),
    momentum: Math.round(95 - i / Math.max(1, secs.length - 1) * 70),
    flowProxy: +(s.chg * 1.4).toFixed(2) }));

  /* Asset class rotation proxies */
  const px = (sym, w) => { const v = q(sym)?.changePct; return v == null ? null : v * w; };
  const eq = idxChgs.length ? idxChgs.reduce((a, b) => a + b, 0) / idxChgs.length : null;
  const bondPx = tnx ? -tnx.changePct * 1.5 : null; // yields up = bond price down
  d.assets = [
    { n: 'EQUITIES', v: eq }, { n: 'BONDS', v: bondPx }, { n: 'GOLD', v: px('GC=F', 1) },
    { n: 'CRYPTO', v: STATE.crypto.bitcoin ? STATE.crypto.bitcoin.chg24h / 2 : null },
    { n: 'OIL', v: px('CL=F', 1) }, { n: 'SILVER', v: px('SI=F', 1) },
    { n: 'CASH', v: eq != null ? -eq * 0.6 : null },
  ].map(a => ({ ...a, v: a.v == null ? null : +a.v.toFixed(2) }));

  /* Sankey v2: typed sources. FII/DII legs are MEASURED (₹cr -> $B @ ~88/USD);
     other pools are price-derived proxies. meta: 'measured'|'proxy' */
  const lastRow = FIIHIST.length ? FIIHIST[FIIHIST.length - 1] : null;
  const CR_PER_B = 8800; // ₹ crore per $1B at ~₹88/USD
  const fiiB = lastRow && lastRow.fii != null ? lastRow.fii / CR_PER_B : null;
  const diiB = lastRow && lastRow.dii != null ? lastRow.dii / CR_PER_B : null;
  // destination weights: correlation-based (historical) or same-day momentum (legacy)
  let dest;
  if (ROUTING_MODE === 'correlation' && CORR_WEIGHTS?.mode === 'correlation') {
    const w = CORR_WEIGHTS.weights;
    dest = CORR_SYMS.map(s => ({ t: s.t, v: w[s.t] ?? 0, c: s.c }))
      .filter(x => x.v > 0.02).sort((a, b) => b.v - a.v);
  } else {
    // Legacy momentum routing (same-day price % drives weights)
    dest = [
      { t: 'US Tech', v: Math.max(0, q('XLK')?.changePct ?? 0), c: 'in' },
      { t: 'Gold', v: Math.max(0, q('GC=F')?.changePct ?? 0), c: 'gold' },
      { t: 'US Treasuries', v: Math.max(0, -(tnx?.changePct ?? 0)), c: 'liq' },
      { t: 'Crypto', v: Math.max(0, (STATE.crypto.bitcoin?.chg24h ?? 0) / 2), c: 'liq' },
      { t: 'Cash / T-Bills', v: eq != null && eq < 0 ? Math.abs(eq) : 0.15, c: 'out' },
    ].filter(x => x.v > 0.02).sort((a, b) => b.v - a.v);
  }
  const wsum = dest.reduce((a, x) => a + x.v, 0) || 1;
  const links = [];
  const route = (src, vol, meta) => { // split a source pool across top destinations
    dest.slice(0, 3).forEach(x => {
      const v = vol * (x.v / wsum);
      if (v > 0.05) links.push({ s: src, t: x.t, v: +v.toFixed(2), c: x.c, m: meta });
    });
  };
  // MEASURED legs
  if (fiiB != null) {
    if (fiiB < 0) route('India Equities — FII Exit', Math.abs(fiiB) * 4, 'measured'); // x4: daily->visual scale
    else links.push({ s: 'Foreign Portfolio (FII)', t: 'India Equities', v: +(fiiB * 4).toFixed(2), c: 'in', m: 'measured' });
  }
  if (diiB != null && diiB > 0)
    links.push({ s: 'Domestic Inst. (DII)', t: 'India Equities', v: +(diiB * 4).toFixed(2), c: 'in', m: 'measured' });
  // PROXY pools
  const fgs = d.fearGreed.score;
  route('US Money Mkt Funds', 1.2 + Math.max(0, (fgs - 50) / 18), 'proxy');
  if ((bondPx ?? 0) < 0) route('Global Bond Funds', 0.8 + Math.abs(bondPx), 'proxy');
  if ((q('^HSI')?.changePct ?? 0) < -0.3) route('China/HK Outflows', Math.abs(q('^HSI').changePct), 'proxy');
  route('Gold ETF Redemptions', (q('GC=F')?.changePct ?? 0) < -0.3 ? Math.abs(q('GC=F').changePct) : 0, 'proxy');
  d.sankey = links.filter(l => l.v > 0.05);
  d.sankeyMeta = {
    routingMode: ROUTING_MODE,
    corrActive: CORR_WEIGHTS?.mode === 'correlation',
    corrStats: (CORR_WEIGHTS?.mode === 'correlation') ? {
      sellWeeks: CORR_WEIGHTS.sellWeeks,             // weeks that actually contributed
      sellWeeksFound: CORR_WEIGHTS.sellWeeksFound,   // sell weeks present in the FII record
      matchedPerAsset: CORR_WEIGHTS.matchedPerAsset,
      windowWeeks: CORR_WEIGHTS.windowWeeks,
      weights: CORR_WEIGHTS.weights,
      computed: CORR_WEIGHTS.computed,
    } : { reason: CORR_WEIGHTS?.reason || 'not yet computed' },
  };

  /* FX: strength score + live rate + change */
  d.fx = [{ n: 'USD', full: 'US Dollar', pair: 'DXY', cc: 'us',
      rate: dxy ? +dxy.price.toFixed(3) : null,
      chg: dxy ? +dxy.changePct.toFixed(3) : null,
      s: dxy ? clamp(50 + dxy.changePct * 18, 5, 95) : null },
    ...FX.map(x => {
      const qt = q(x.s); const v = qt?.changePct;
      const adj = x.inv ? -(v ?? 0) : (v ?? 0);
      /* normalise rate to convention: EUR/USD → 1.08, USD/JPY → 155 etc */
      let rate = qt?.price ?? null;
      if (rate != null) rate = +rate.toFixed(x.inv ? (rate > 100 ? 2 : 4) : 4);
      return { n: x.n, full: x.full, pair: x.pair, cc: x.cc,
        rate, chg: v != null ? +v.toFixed(3) : null,
        s: v != null ? clamp(50 + adj * 18, 5, 95) : null };
    })]
    .map(x => ({ ...x, s: x.s == null ? null : Math.round(x.s) }));

  /* Curve */
  const dgs2 = STATE.fred.data.DGS2?.[0]?.v, dgs10 = STATE.fred.data.DGS10?.[0]?.v;
  d.curve = {
    us3m: q('^IRX') ? q('^IRX').price : null, us5y: q('^FVX') ? q('^FVX').price : null,
    us10y: tnx ? tnx.price : null, us30y: q('^TYX') ? q('^TYX').price : null,
    s2s10: (dgs2 != null && dgs10 != null) ? +((dgs10 - dgs2) * 100).toFixed(0) : null, // bp, FRED daily
  };

  /* World economies: drives geo map + heatmap */
  d.world = INDICES.filter(x => x.cc !== 'US' || x.t12).filter((x, idx, a) => a.findIndex(y => y.cc === x.cc) === idx || x.t12)
    .map(x => ({ ...x, ll: x.ll || null, price: q(x.s)?.price ?? null, chg: q(x.s)?.changePct ?? null }))
    .map(({ s, n, cc, geo, t12, ll, price, chg }) => ({ s, n, cc, geo: geo ?? null, t12: !!t12, ll, price, chg }));
  /* full heatmap list = every distinct economy incl HK/SG */
  d.worldAll = INDICES
    .map(x => ({ n: x.n, cc: x.cc, geo: x.geo ?? null, price: q(x.s)?.price ?? null, chg: q(x.s)?.changePct ?? null }));
  /* India tab */
  const lastRowI = FIIHIST.length ? FIIHIST[FIIHIST.length - 1] : null;
  d.india = {
    idx: INDIA_IDX.map(x => ({ n: x.n, price: q(x.s)?.price ?? null, chg: q(x.s)?.changePct ?? null, spark: q(x.s)?.spark ?? null })),
    sectors: INDIA_SECTORS.map(x => ({ n: x.n, price: q(x.s)?.price ?? null, chg: q(x.s)?.changePct ?? null }))
      .filter(x => x.chg != null).sort((a, b) => b.chg - a.chg),
    /* Prefer NSE's authoritative constituent list; fall back to the static snapshot
       only if NSE has not responded yet (e.g. first seconds after a cold start). */
    nifty50: N50_LIVE.length
      ? N50_LIVE.map(x => ({ n: x.n, w: N50_W[x.n] ?? 0.5, price: x.price, chg: x.chg }))
      : NIFTY50.map(x => ({ n: x.n, w: x.w, price: q(x.s)?.price ?? null, chg: q(x.s)?.changePct ?? null })),
    usdinr: { price: q('INR=X')?.price ?? null, chg: q('INR=X')?.changePct ?? null },
    fiiLatest: lastRowI,
  };
  /* India insight engine: rule-based on measured + live data */
  const iIns = [];
  if (FIIHIST.length > 3) {
    let n = 0, cum = 0;
    for (let k = FIIHIST.length - 1; k >= 0 && (FIIHIST[k].fii ?? 0) * (FIIHIST[FIIHIST.length - 1].fii ?? 0) > 0; k--) { n++; cum += FIIHIST[k].fii; }
    const selling = (FIIHIST[FIIHIST.length - 1].fii ?? 0) < 0;
    iIns.push({ c: selling ? 'bear' : 'bull', t: `FII ${selling ? 'selling' : 'buying'} streak: ${n} session${n > 1 ? 's' : ''}`,
      d: `Cumulative ${cum >= 0 ? '+' : ''}₹${Math.round(cum).toLocaleString('en-IN')} cr over the streak (NSE cash, provisional). ${lastRowI && lastRowI.dii != null ? `DII ${lastRowI.dii >= 0 ? 'absorbed +' : ''}₹${Math.round(lastRowI.dii).toLocaleString('en-IN')} cr on ${lastRowI.d}.` : ''}` });
  }
  const isec = d.india.sectors;
  if (isec.length > 2) iIns.push({ c: 'bull', t: `${isec[0].n} leads, ${isec[isec.length - 1].n} lags`,
    d: `${isec[0].n} ${isec[0].chg >= 0 ? '+' : ''}${isec[0].chg.toFixed(2)}% vs ${isec[isec.length - 1].n} ${isec[isec.length - 1].chg.toFixed(2)}% — ${(isec[0].chg - isec[isec.length - 1].chg).toFixed(1)}pp sector dispersion on NSE today.` });
  const ivix = q('^INDIAVIX');
  if (ivix) iIns.push({ c: ivix.price > 20 ? 'bear' : ivix.price < 13 ? 'gold' : '', t: `India VIX ${ivix.price.toFixed(1)} — ${ivix.price > 20 ? 'elevated risk pricing' : ivix.price < 13 ? 'complacency zone' : 'normal range'}`,
    d: `Day move ${ivix.changePct >= 0 ? '+' : ''}${ivix.changePct.toFixed(1)}%. ${ivix.price < 13 ? 'Options cheap; hedging costs near lows.' : ivix.price > 20 ? 'Expect wider intraday ranges.' : 'Volatility regime stable.'}` });
  const inr = q('INR=X');
  if (inr) iIns.push({ c: inr.changePct > 0.15 ? 'bear' : '', t: `Rupee ${inr.changePct > 0 ? 'weaker' : 'firmer'} at ${inr.price.toFixed(2)}`,
    d: `USD/INR ${inr.changePct >= 0 ? '+' : ''}${inr.changePct.toFixed(2)}% today. ${inr.changePct > 0.15 ? 'Depreciation pressure typically accompanies FII outflows.' : 'Stable currency supports foreign positioning.'}` });
  d.india.insights = iIns;

  /* Regime + rule-based insights from real values */
  const fg = d.fearGreed.score;
  d.regime = fg >= 50 ? 'RISK-ON' : 'RISK-OFF';
  const ins = [];
  if (vix) ins.push({
    c: fg >= 55 ? 'bull' : fg <= 45 ? 'bear' : '',
    t: fg >= 55 ? 'Risk-on regime' : fg <= 45 ? 'Risk-off regime' : 'Neutral regime',
    d: `Fear & Greed ${fg} (scored vs trailing averages). VIX ${vix.price.toFixed(1)}`
      + (H.vixMA50 ? ` vs 50d avg ${H.vixMA50.toFixed(1)}` : '')
      + `, ${Math.round(breadthScore)}% of tracked indices above their 20d average`
      + `, DXY ${dxy ? (dxy.changePct >= 0 ? '+' : '') + dxy.changePct.toFixed(2) + '%' : 'n/a'}.` });
  if (d.sectors.length) ins.push({ c: 'bull', t: `${d.sectors[0].n} leads sector tape`,
    d: `${d.sectors[0].n} ${d.sectors[0].chgPct >= 0 ? '+' : ''}${d.sectors[0].chgPct}% vs ${d.sectors[d.sectors.length - 1].n} ${d.sectors[d.sectors.length - 1].chgPct}% — ${(d.sectors[0].chgPct - d.sectors[d.sectors.length - 1].chgPct).toFixed(1)}pp dispersion (ETF proxy).` });
  const au = q('GC=F');
  if (au) ins.push({ c: 'gold', t: au.changePct >= 0 ? 'Gold bid' : 'Gold offered',
    d: `Gold ${au.changePct >= 0 ? '+' : ''}${au.changePct.toFixed(2)}% at $${au.price.toFixed(0)}/oz while equities ${eq != null ? (eq >= 0 ? 'rise' : 'fall') : 'trade'} — ${au.changePct > 0 && (eq ?? 0) > 0 ? 'parallel bid suggests liquidity-driven tape' : 'classic risk rotation'}.` });
  if (STATE.fii.data?.length) { const fii = STATE.fii.data.find(x => /FII|FPI/i.test(x.category));
    if (fii) ins.push({ c: +fii.netValue >= 0 ? 'bull' : 'bear', t: `India FII net ${+fii.netValue >= 0 ? 'buyers' : 'sellers'} (${fii.date})`,
      d: `FII/FPI net ₹${(+fii.netValue).toLocaleString('en-IN')} cr (NSE provisional, T+1). DII: ₹${(+(STATE.fii.data.find(x => /DII/i.test(x.category))?.netValue || 0)).toLocaleString('en-IN')} cr.` }); }
  d.insights = ins;

  STATE.derived = d;
}

/* ---------------- snapshot assembly ---------------- */
function snapshot() {
  const pick = arr => arr.map(x => ({ ...x, q: STATE.quotes[x.s] || null }));
  return {
    ts: Date.now(),
    latency: { prices: 'LIVE ~20s', crypto: 'LIVE ~30s', fii: 'T+1 DAILY', fred: 'DAILY/WEEKLY', derived: 'DERIVED PROXY', nseRT: 'LIVE ~5s', treasury: 'OFFICIAL DAILY' },
    indices: pick(INDICES), macro: pick(MACRO), bonds: pick(BONDS),
    coms: pick(COMS), sectorsRaw: pick(SECTORS), fxRaw: pick(FX),
    crypto: STATE.crypto, cgGlobal: STATE.cgGlobal,
    fii: STATE.fii, se: STATE.se, fiiHistCount: FIIHIST.length,
    backfill: SE_BF, fredEnabled: STATE.fred.enabled, fredError: STATE.fred.error,
    derived: STATE.derived, pollerStatus: STATE.pollerStatus,
  };
}

/* ---------------- SSE ---------------- */
const sseClients = new Set();
function broadcast() {
  const msg = `data: ${JSON.stringify(snapshot())}\n\n`;
  for (const res of sseClients) { try { res.write(msg); } catch { sseClients.delete(res); } }
}

/* ---------------- cold-start warm-up ------------------------------------------------
 * On a host that suspends the process between requests, the first visitor after an
 * idle spell hit /api/snapshot while STATE.quotes was still empty — the dashboard
 * rendered all em-dashes and only filled in once SSE pushed the first poll. That is
 * the "data sometimes not refreshed while opening" symptom.
 *
 * warmUp() runs a bounded refresh of the panels that matter most on first paint.
 * Concurrent callers share one in-flight run, and the race against a timer means a
 * slow upstream can never hang the request — it just returns whatever has landed.
 * -----------------------------------------------------------------------------------*/
let LAST_POLL = 0;
let WARMING = null;
function warmUp(maxMs = 6000) {
  if (WARMING) return WARMING;                       // coalesce simultaneous visitors
  WARMING = Promise.race([
    Promise.all([
      pollYahoo(FAST_YH).catch(() => {}),
      pollNSEQuotes().catch(() => {}),
      pollNSEStocks().catch(() => {}),
      pollComs().catch(() => {}),
      /* FRED must be in here. Without it a cold start served a fully-populated
         dashboard whose Fear & Greed credit factor was still the hardcoded 50
         placeholder — a fabricated fifth of a published score. Skipped once
         loaded, since these series only change daily/weekly. */
      (STATE.fred.data.BAMLH0A0HYM2?.length ? Promise.resolve()
        : pollFRED().then(pollTreasury).catch(() => {})),
      /* Trailing averages for relative Fear & Greed scoring. Without these the
         index silently falls back to fixed absolute bands, which is the scoring
         that read greed while CNN read fear. Cached 6h, so this is a no-op once warm. */
      (STATE.fgHist?.vixMA50 ? Promise.resolve() : pollFGHistory().catch(() => {})),
    ]),
    new Promise(r => setTimeout(r, maxMs)),
  ]).then(() => { derive(); LAST_POLL = Date.now(); })
    .catch(() => {})
    .finally(() => { WARMING = null; });
  return WARMING;
}
function dataIsCold() {
  return Object.keys(STATE.quotes).length < 20 || !LAST_POLL || Date.now() - LAST_POLL > 120_000;
}

/* ---------------- HTTP server ---------------- */
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.json': 'application/json' };
/* no-store on the live endpoints: an edge cache in front of this would happily serve
   a several-minute-old snapshot, which looks identical to the app being broken. */
const LIVE_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Cache-Control': 'no-store, no-cache, must-revalidate',
};
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/api/snapshot') {
    if (dataIsCold()) {
      return warmUp().then(() => {
        res.writeHead(200, LIVE_HEADERS);
        res.end(JSON.stringify(snapshot()));
      });
    }
    res.writeHead(200, LIVE_HEADERS);
    return res.end(JSON.stringify(snapshot()));
  }
  if (url === '/api/fii-history') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ rows: FIIHIST, count: FIIHIST.length }));
  }
  if (url === '/api/fii-import' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', () => {
      const n = importText(body);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ imported: n, total: FIIHIST.length }));
    });
    return;
  }
  if (url.startsWith('/api/hist')) {
    const qs = new URLSearchParams(req.url.split('?')[1]||'');
    const sym = qs.get('s'), range = qs.get('range')||'3y';
    if (!sym) { res.writeHead(400); return res.end('missing s'); }
    fetchYahooHist(sym, range).then(closes => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=3600' });
      res.end(JSON.stringify({ sym, range, closes: closes || [] }));
    }).catch(() => { res.writeHead(500); res.end('{}'); });
    return;
  }
  if (url === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ up: Date.now() - STATE.startedAt, pollers: STATE.pollerStatus, sseClients: sseClients.size }));
  }
  if (url === '/api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' });
    res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
    return;
  }
  if (url === '/api/routing-mode') {
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      return res.end(JSON.stringify({ mode: ROUTING_MODE, corrWeights: CORR_WEIGHTS }));
    }
    if (req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const { mode } = JSON.parse(body);
          if (mode === 'correlation' || mode === 'momentum') {
            ROUTING_MODE = mode;
            try { fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
              fs.writeFileSync(ROUTING_MODE_FILE, JSON.stringify({ mode })); } catch {}
            derive(); broadcast();
            res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            res.end(JSON.stringify({ mode: ROUTING_MODE, ok: true }));
          } else { res.writeHead(400); res.end('invalid mode'); }
        } catch { res.writeHead(400); res.end('bad json'); }
      });
      return;
    }
  }
  if (url === '/api/news') {
    if (!NEWS_CACHE.ts || Date.now() - NEWS_CACHE.ts > NEWS_TTL) pollNews().catch(() => {});
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'max-age=300' });
    return res.end(JSON.stringify({ global: NEWS_CACHE.global, india: NEWS_CACHE.india, ts: NEWS_CACHE.ts }));
  }
  // static
  let fp = path.join(__dirname, 'public', url === '/' ? 'index.html' : url);
  if (!fp.startsWith(path.join(__dirname, 'public'))) { res.writeHead(403); return res.end(); }
  fs.readFile(fp, (e, buf) => {
    if (e) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream',
      'Cache-Control': 'no-cache, must-revalidate', 'Access-Control-Allow-Origin': '*' });
    res.end(buf);
  });
});

/* ---------------- scheduler ---------------- */
async function fastCycle() { await pollYahoo(FAST_YH); LAST_POLL = Date.now(); derive(); broadcast(); }
async function slowCycle() { await pollYahoo(SLOW_YH); LAST_POLL = Date.now(); derive(); broadcast(); }
async function cryptoCycle() { await pollCG(); derive(); broadcast(); }
(async () => {
  console.log(`FlowAtlas backend starting on :${PORT}  (FRED ${FRED_KEY ? 'enabled' : 'DISABLED — set FRED_API_KEY'}) (Finnhub ${FINNHUB_KEY ? 'enabled' : 'DISABLED — set FINNHUB_API_KEY'})`);

  /* Bind the port FIRST. Previously this waited on the initial Yahoo + CoinGecko +
     commodities pass — tens of seconds of network I/O before the socket was open.
     On a host that expects a fast ready signal (Vercel) that means the function is
     killed before it ever listens. The dashboard degrades gracefully while the
     first poll completes, so there is no reason to block on it. */
  server.listen(PORT, () => console.log(`✓ http://localhost:${PORT}`));

  /* Everything below is fire-and-forget; each poller has its own try/catch and
     broadcasts to connected clients as its data lands. */
  pollFRED().then(pollTreasury).catch(e => console.error('fred/treasury:', e.message));
  pollNSE(); pollSE().then(() => { backfillSE().then(() => computeCorrelationWeights()); });
  pollNSEQuotes(); pollNSEStocks(); pollNews();
  pollFGHistory();                              // trailing averages for relative F&G
  setInterval(pollFGHistory, 60 * 60_000);      // refresh hourly; underlying cache is 6h
  computeCorrelationWeights();
  setInterval(computeCorrelationWeights, 24 * 60 * 60_000);

  Promise.all([pollYahoo(ALL_YH), pollCG(), pollComs()])
    .then(() => { derive(); broadcast(); if (FINNHUB_KEY) pollFinnhub(); })
    .catch(e => console.error('initial poll:', e.message));

  setInterval(fastCycle, 6_000);
  setInterval(slowCycle, 30_000);
  setInterval(pollComs, 30_000);   // commodity futures every 30s
  setInterval(cryptoCycle, 120_000);  // CoinGecko free tier — see pollCG() notes
  setInterval(pollNSEQuotes, 5_000);        // NSE indices real-time every 5s
  setInterval(pollNSEStocks, 10_000);       // Nifty-50 constituents every 10s
  setInterval(pollNSE, 15 * 60_000);
  setInterval(pollSE, 30 * 60_000);
  setInterval(() => pollFRED().then(pollTreasury).catch(() => {}), 60 * 60_000);
  setInterval(pollNews, NEWS_TTL);
  if (FINNHUB_KEY) setInterval(pollFinnhub, 2 * 60_000); // Finnhub fallback every 2 min
})();

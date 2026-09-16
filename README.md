<div align="center">

# FlowAtlas

**Real-time global capital intelligence terminal**

Track where money moves across world markets — live, every 15 seconds.

*Built by [CA Gaurav N. Makwana](https://github.com/)*

</div>

---

## What it does

FlowAtlas is a self-hosted markets dashboard that pulls live data from public sources and renders it as a single-screen terminal. Six dashboards in one app:

| Tab | What's in it |
|---|---|
| **Global Pulse** | Live 3D globe of world markets, Global Fear & Greed Index, top movers |
| **Flow Engine** | FII/DII money-flow Sankey with correlation-based routing, asset class rotation |
| **World Markets** | 46 global indices, regional heatmaps |
| **Indian Markets** | Nifty / Sensex / Bank Nifty / India VIX, NSE FII-DII flows, sector bars, Nifty 50 heatmap |
| **Sectors & FX** | Global sector rotation, commodity heatmap, live FX rates, cross-rate matrix |
| **Latest News** | Live India + global financial headlines |

Dark and light mode. Fully responsive on mobile.

---

## Quick start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/flowatlas.git
cd flowatlas

# 2. Install (single dependency)
npm install

# 3. Add your FRED key (free — needed for credit spreads and the yield curve)
cp .env.example .env
#   then open .env and paste your key

# 4. Run
npm start
```

Open **http://localhost:8787**

Get a free FRED API key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) — takes about a minute.

---

## Endpoints

| Route | Purpose |
|---|---|
| `/` | Dashboard UI |
| `/api/snapshot` | Full state JSON |
| `/api/stream` | SSE live updates (~20s cadence) |
| `/api/health` | Poller status + data ages |
| `/api/routing-mode` | GET/POST the Sankey routing mode |

---

## Data honesty ledger

Read this before trusting any panel.

| Panel | Source | Latency | What it actually is |
|---|---|---|---|
| Indices, commodities, FX, US yields | Yahoo Finance spark | ~20s poll; some venues delayed up to 15 min | Real prices |
| Crypto | CoinGecko free | ~30s poll | Real prices / mcap |
| Fear & Greed | VIX, US10Y, DXY, breadth, HY OAS | live inputs | **DERIVED** — formula is in `derive()` |
| HY credit spread, 2s10s curve | FRED: BAMLH0A0HYM2, DGS2, DGS10 | daily | Real series; needs FRED key |
| India FII/DII | NSE provisional | **T+1, after close** | The only measured *flow* in the free stack |
| Sankey, rotation, sector radar | Price / sector-ETF moves | live inputs | **PROXIES, not measured fund flows** — labeled DERIVED in the UI |
| Crypto exchange flows / whales | not included | — | Needs Glassnode / CryptoQuant (paid). Omitted rather than faked |

---

## Known limits

These are deliberate, not bugs.

- **Yahoo endpoints are unofficial.** Stable for years at a time, but can change without notice. The poller fails soft per chunk.
- **"Real-time" means seconds-to-minutes**, not ticks. True tick data requires paid feeds.
- **NSE blocks datacenter IPs.** FII/DII data works from Indian residential IPs and fails soft elsewhere — so on most cloud hosts that panel will stay empty.
- **2s10s spread needs a FRED key** (DGS2 has no free live source).
- **India / Germany / Japan 10Y yields** have no reliable free live feed and are intentionally not shown.

---

## Deploying

Works on any Node host. Railway, Render, and Fly.io all have free tiers.

```
Build command:  npm install
Start command:  npm start
Env vars:       FRED_API_KEY, PORT
```

Note the NSE limitation above — on cloud hosts the FII/DII panel will not populate.

---

## Roadmap

1. ~~FRED key → credit spreads + 2s10s live~~ ✅
2. ~~Correlation-based Sankey routing~~ ✅
3. ~~Mobile responsive + light mode~~ ✅
4. Deploy near an Indian IP → NSE FII/DII activates
5. AMFI / NSDL FPI data + ETF flow files → replace proxies with measured flows
6. Redis + TimescaleDB → 1W / 1M / YTD timeframes from captured history

---

## Tech

Node 18+ · Zero framework · Vanilla JS frontend · Three.js for the globe · Single dependency (`dotenv`)

## License

MIT

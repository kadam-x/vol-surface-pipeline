# Technikai dokumentáció — Equity Derivatives Market Data & Implied Volatility Surface Pipeline

---

## 1. Választott architektúra és indoklás

A projekt célja egy napi rendszerességgel futó, end-to-end adatmérnöki pipeline megvalósítása, amely részvényopciók piaci adatait tölti be, azokból implied volatility (IV) felületet számít, és az eredményt REST API-n, valamint Grafana dashboardon keresztül teszi elérhetővé.

### Architektúra áttekintés

```
[yfinance API]──┐
                ├──► [MinIO Landing Zone] ──► [Transform + IV Calc] ──► [DuckDB Warehouse]
[FRED API]──────┘                                                               │
                                                                         [FastAPI REST]
                                                                         [Grafana Dashboard]
```

A pipeline hat logikai rétegre tagolódik: adatbetöltés, nyers tárolás, transzformáció, adattárház, kiszolgálás és vizualizáció. Az egyes rétegek elkülönítése tudatos tervezési döntés — az egyik réteg cserélhetővé válik anélkül, hogy a többi érintett lenne. Például a MinIO helyett bármely S3-kompatibilis objektumtároló (pl. AWS S3) bekötéssel leváltható, a DuckDB helyett Snowflake vagy BigQuery csatlakoztatható anélkül, hogy a transzformációs logikát módosítani kellene.

### Eszközválasztás indoklása

**MinIO** — S3-kompatibilis objektumtároló a nyers landing zone-hoz. A választás indoka, hogy a valós production környezetekben a nyers adatok objektumtárolóba (pl. S3, GCS) kerülnek, nem fájlrendszerbe. A MinIO ezt lokálisan reprodukálja, így az architektúra felhőre migrálható módosítás nélkül.

**DuckDB** — analitikai adattárház. Beágyazott, szervermentes megoldás, amely natívan kezeli a Parquet fájlokat és rendkívül gyors analitikai lekérdezéseket tesz lehetővé. A csillag sémát SQL-ben definiáltuk, a FK-kapcsolatokkal együtt. A DuckDB választása indokolt egy lokális, reprodukálható pipeline esetén — a séma minimális módosítással Snowflake-re vagy BigQuery-re portolható.

**Apache Airflow** — orkesztráció. Az iparági standard workflow-kezelő eszköz, amely DAG-alapú függőségkezelést, ütemezést és újrafuttathatóságot biztosít. A pipeline idempotens: ugyanazon dátumra való újrafuttatás törlés + újrainzertálással kezeli a duplikációt.

**FastAPI** — adatkiszolgálás. Aszinkron, production-grade Python REST framework. A választás indoka, hogy a quant dev környezetekben az analitikai adatokat API-n keresztül szolgálják ki a downstream rendszerek (pl. risk rendszerek, trading dashboardok) felé — nem közvetlen DB-kapcsolaton.

**SciPy Brent-módszer** — IV számítás. A Black-Scholes egyenlet invertálása zárt formában nem lehetséges, ezért numerikus gyökkereső algoritmust alkalmazunk. A Brent-módszer garantáltan konvergál, ha a gyök az intervallumon belül van, és robusztusabb az alternatív Newton-Raphson módszernél, amely deep ITM/OTM opcióknál divergálhat.

---

## 2. Adatforrások

### 2.1 yfinance — opciós lánc

A yfinance könyvtár a Yahoo Finance API-t wrappolja. Minden futás során lekéri az összes elérhető opciót (call + put) az összes lejáratra és strike szintre. A visszaadott adat semi-strukturált JSON formátumban érkezik, amelyet Pandas DataFrame-mé alakítunk.

**Lekért mezők:** strike, expiry_date, lastPrice, bid, ask, volume, openInterest

**Tőzsdei szimbólumok:** SPY (S&P 500 ETF) — a leglikvidebb opciós piac, tiszta adatminőség

### 2.2 FRED API — kockázatmentes kamatláb

A St. Louis Federal Reserve FRED API-ján keresztül lekért US 10-éves Treasury hozam (DGS10 sorozat) szolgál kockázatmentes kamatláb proxiként a Black-Scholes számításban. Ha az API nem elérhető, a pipeline egy konfigurálható fallback értékkel fut tovább.

---

## 3. Adatmodell — Csillag séma

```
                    ┌─────────────┐
                    │  dim_ticker │
                    │─────────────│
                    │ ticker_id PK│
                    │ symbol      │
                    │ sector      │
                    │ exchange    │
                    └──────┬──────┘
                           │
┌─────────────┐     ┌──────▼──────────────────┐     ┌─────────────┐
│  dim_expiry │     │  fact_options_snapshot  │     │  dim_strike │
│─────────────│     │─────────────────────────│     │─────────────│
│ expiry_id PK│◄────│ snapshot_id PK          │────►│ strike_id PK│
│ expiry_date │     │ ticker_id FK            │     │ strike_price│
│ days_to_exp │     │ expiry_id FK            │     │ moneyness   │
│ expiry_buck │     │ strike_id FK            │     └─────────────┘
└─────────────┘     │ option_type             │
                    │ implied_vol             │
                    │ last_price              │
                    │ mid_price               │
                    │ bid / ask               │
                    │ volume                  │
                    │ open_interest           │
                    │ underlying_price        │
                    │ risk_free_rate          │
                    │ snapshot_date           │
                    └─────────────────────────┘
```

**Tervezési döntések:**

A `fact_options_snapshot` tábla napi snapshotokat tartalmaz — minden futás egy új `snapshot_date` értéket szúr be, a korábbi napok adatai megmaradnak. Ez lehetővé teszi az IV időbeli alakulásának elemzését.

A `dim_strike` tartalmazza a `moneyness_bucket` mezőt (ITM/ATM/OTM), amelyet az underlying árfolyamhoz viszonyítva számítunk, opciótípus-függően (call és put esetén a logika tükrözött).

A `dim_expiry` tartalmazza a `days_to_expiry` és `expiry_bucket` (near/mid/far) mezőket, amelyek a term structure elemzését segítik anélkül, hogy minden lekérdezésben dátumszámítást kellene végezni.

---

## 4. Transzformációs pipeline

### 4.1 Adattisztítás (`clean.py`)

- Null értékek kezelése: strike, last_price, underlying_price hiánya esetén a sor törlődik
- Típuskonverzió: minden numerikus mező explicit `pd.to_numeric(..., errors='coerce')` hívással konvertálódik
- Mid price számítás: `(bid + ask) / 2`, fallback: `last_price` ha bid/ask nulla
- Lejárt opciók kiszűrése: `days_to_expiry <= 0` sorok törlődnek
- Moneyness és expiry bucket hozzáadása

### 4.2 Implied Volatility számítás (`iv_surface.py`)

A Black-Scholes egyenlet invertálása SciPy `brentq` gyökkereső algoritmussal történik. Minden opció sorára:

1. Intrinsic value ellenőrzés — ha a piaci ár intrinsic value alatt van, az IV nem értelmes (arbitrázs), a sor törlődik
2. Bracket ellenőrzés — ha a `[1e-6, 20.0]` intervallumon nem zárható be a gyök, a sor törlődik
3. Brent-módszer futtatása — garantált konvergencia az intervallumon belül
4. Sanity filter — 1% alatti vagy 500% feletti IV értékek törlődnek (valószínűleg hibás quote)

A végeredményben az összes opció IV értéke megtalálható, amelyeket együttesen **volatility surface**-nek nevezünk.

### 4.3 Aggregációk

A FastAPI term-structure endpointja ATM opciókra átlagolja az IV értékeket expiry bucket szinten — ez az iparágban standard "volatility term structure" görbe.

---

## 5. Orchestration

Az Airflow DAG (`options_vol_surface_daily`) hétköznaponként 18:00 UTC-kor fut:

```
start
  ├── fetch_options_chain ──► upload_raw_to_minio ──► transform_and_compute_iv
  └── fetch_macro_rates ──────────────────────────────────────────────────────┘
                                                              │
                                                         load_duckdb
                                                              │
                                                     validate_data_quality
                                                              │
                                                             end
```

**Idempotencia:** a `load_duckdb` task futás előtt törli az adott `snapshot_date`-hez tartozó fact tábla sorokat, majd újraszúrja azokat. Így ugyanazon napra való újrafuttatás nem eredményez duplikációt.

**Validáció:** a `validate_data_quality` task ellenőrzi, hogy az IV számítás sikerességi aránya legalább 50% — ha nem, a DAG run hibával zárul.

---

## 6. Adatkiszolgálás

### REST API (FastAPI)

| Endpoint | Leírás |
|---|---|
| `GET /health` | Service és DB állapot |
| `GET /tickers` | Elérhető szimbólumok listája |
| `GET /vol-surface/{ticker}` | Teljes IV felület |
| `GET /term-structure/{ticker}` | ATM IV lejárat szerint |
| `GET /vol-smile/{ticker}/{expiry}` | Vol smile egy lejáratra |
| `GET /snapshots/{ticker}` | Elérhető snapshot dátumok |

### Grafana dashboard

A Grafana Infinity datasource-on keresztül közvetlenül a FastAPI endpointokat kérdezi le. Három panel:

- **Vol Smile** — IV strike függvényében egy adott lejáratra (XY chart)
- **Term Structure** — ATM IV lejárati bucket szerint (bar chart)
- **IV Surface heatmap** — moneyness vs expiry bucket, szín = IV érték

### Analitikai lekérdezések (`queries/`)

**ATM vol idősor** (`atm_vol_timeseries.sql`):
```sql
SELECT
    f.snapshot_date,
    dt.symbol,
    AVG(f.implied_vol) AS atm_iv
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
WHERE ds.moneyness_bucket = 'ATM'
  AND f.implied_vol IS NOT NULL
GROUP BY f.snapshot_date, dt.symbol
ORDER BY f.snapshot_date, dt.symbol;
```

**Vol smile** (`vol_smile.sql`):
```sql
SELECT
    ds.strike_price,
    ds.moneyness_bucket,
    f.option_type,
    f.implied_vol,
    f.bid,
    f.ask
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE dt.symbol = 'SPY'
  AND de.expiry_date = '2026-04-17'
  AND f.snapshot_date = '2026-04-12'
  AND f.implied_vol IS NOT NULL
ORDER BY ds.strike_price;
```

**Cross-ticker vol összehasonlítás** (`cross_ticker_comparison.sql`):
```sql
SELECT
    dt.symbol,
    de.expiry_bucket,
    AVG(f.implied_vol) AS avg_iv,
    MIN(f.implied_vol) AS min_iv,
    MAX(f.implied_vol) AS max_iv
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE ds.moneyness_bucket = 'ATM'
  AND f.implied_vol IS NOT NULL
  AND f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_options_snapshot)
GROUP BY dt.symbol, de.expiry_bucket
ORDER BY dt.symbol, de.expiry_bucket;
```

**Term structure** (`term_structure.sql`):
```sql
SELECT
    de.expiry_bucket,
    de.days_to_expiry,
    de.expiry_date,
    AVG(f.implied_vol) AS avg_atm_iv
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE dt.symbol = 'SPY'
  AND ds.moneyness_bucket = 'ATM'
  AND f.implied_vol IS NOT NULL
  AND f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_options_snapshot)
GROUP BY de.expiry_bucket, de.days_to_expiry, de.expiry_date
ORDER BY de.days_to_expiry;
```

---

## 7. Pipeline futásának eredménye

*(Ide kerülnek a screenshotok és lekérdezési eredmények)*

- Airflow DAG — sikeres futás (minden task zöld)
- DuckDB sorok száma: **16 264 sor** (SPY, 2 snapshot dátum)
- FastAPI health check: `{"status": "healthy", "db": "connected"}`
- Grafana dashboard — vol smile és term structure panelek

---

## 8. Infrastruktúra

A teljes környezet egyetlen paranccsal indítható:

```bash
docker compose build && docker compose up -d
```

Négy service fut Docker Compose alatt:

| Service | Image | Feladat |
|---|---|---|
| `airflow` | custom (apache/airflow:2.8.1) | Orchestration, pipeline futtatás |
| `minio` | minio/minio:latest | Raw landing zone |
| `fastapi` | custom (python:3.11-slim) | REST API kiszolgálás |
| `grafana` | grafana/grafana:latest | Dashboard vizualizáció |

A DuckDB fájl Docker volume-on él (`duckdb_data`), így az adatok persistensek a container újraindítások között. Minden konfiguráció `.env` fájlból olvasódik — nincs hardcoded érték a forráskódban.

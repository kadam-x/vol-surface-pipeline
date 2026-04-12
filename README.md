# Equity Derivatives Market Data & Implied Volatility Surface Pipeline

Napi batch pipeline, amely részvényopciók piaci adatait tölti be, implied volatility felületet számít Black-Scholes inverziót alkalmazva, és az eredményt REST API-n valamint Grafana dashboardon keresztül teszi elérhetővé.

A projekt a BME *Data Engineering a gyakorlatban* tárgy opcionális házi feladata keretében készült.

---

## Architektúra

```
[yfinance API]──┐
                ├──► [MinIO Landing Zone] ──► [Transzformáció + IV számítás] ──► [DuckDB Warehouse]
[FRED API]──────┘                                                                        │
                                                                                  [FastAPI REST]
                                                                                  [Grafana Dashboard]
```

A pipeline Apache Airflow alatt fut, hétköznaponként 18:00 UTC-kor. A nyers opciós adatok Parquet formátumban kerülnek MinIO-ba, a transzformáció Pandas és SciPy segítségével történik, az eredmény egy csillag sémájú DuckDB adattárházba töltődik, és FastAPI végpontokon keresztül kiszolgált.

---

## Technológiai stack

| Réteg | Eszköz |
|---|---|
| Betöltés | Python, yfinance, FRED API |
| Landing zone | MinIO (S3-kompatibilis) + Parquet |
| Adattárház | DuckDB |
| Transzformáció | Pandas, SciPy (Brent-módszer) |
| Orchestration | Apache Airflow |
| Kiszolgálás | FastAPI |
| Dashboard | Grafana (Infinity datasource) |
| Infrastruktúra | Docker Compose |

---

## Előfeltételek

- Docker
- Docker Compose v2
- Git

Egyéb függőség nem szükséges — minden a konténereken belül fut.

---

## Indítás lépései

**1. Repository klónozása**

```bash
git clone <repo-url>
cd vol-surface-pipeline
```

**2. Környezeti változók beállítása**

```bash
cp .env.example .env
```

Az `.env` fájlban igény szerint módosíthatók a belépési adatok és a FRED API kulcs. FRED API kulcs nélkül is fut a pipeline — fallback kamatlábat használ.

**3. Build és indítás**

```bash
docker compose build
docker compose up -d
```

Az első build 3-5 percet vesz igénybe, amíg a függőségek települnek az Airflow image-be.

**4. Várakozás az inicializálásra**

```bash
docker compose logs airflow -f
```

A `Scheduler started` üzenet megjelenéséig kell várni, általában 20-30 másodperc.

**5. Airflow UI megnyitása**

`http://localhost:8080` — belépés: `admin` / `admin`

Ha az első indításkor a felhasználó még nem jött létre, manuálisan létrehozható:

```bash
docker exec -it vol-surface-airflow airflow users create \
  --role Admin --username admin --email admin@example.com \
  --password admin --firstname Admin --lastname User
```

**6. Pipeline futtatása**

Az Airflow UI-ban keressük meg az `options_vol_surface_daily` DAG-ot, kapcsoljuk be (kék toggle), majd kattintsunk a ▶ gombra → Trigger DAG.

Vagy parancssorból:

```bash
docker exec -it vol-surface-airflow airflow dags trigger options_vol_surface_daily
```

**7. Adatok ellenőrzése**

```bash
docker exec -it vol-surface-airflow python3 -c "
import duckdb
conn = duckdb.connect('/data/duckdb/vol_surface.db', read_only=True)
print('sorok:', conn.execute('SELECT COUNT(*) FROM fact_options_snapshot').fetchone()[0])
print('tőzsdei szimbólumok:', conn.execute('SELECT symbol FROM dim_ticker').fetchall())
print('snapshot dátumok:', conn.execute('SELECT DISTINCT snapshot_date FROM fact_options_snapshot').fetchall())
"
```

**8. API tesztelése**

```bash
curl http://localhost:8000/health
curl http://localhost:8000/tickers
curl http://localhost:8000/vol-surface/SPY
```

**9. Grafana megnyitása**

`http://localhost:3000` — belépés: `admin` / `admin`

Az Infinity datasource előre be van konfigurálva a FastAPI lekérdezéséhez.

---

## Szolgáltatások

| Szolgáltatás | URL | Belépési adatok |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| FastAPI | http://localhost:8000 | — |
| FastAPI dokumentáció | http://localhost:8000/docs | — |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO konzol | http://localhost:9001 | minioadmin / minioadmin |

---

## API végpontok

| Metódus | Végpont | Leírás |
|---|---|---|
| GET | `/health` | Service és DB állapot ellenőrzése |
| GET | `/tickers` | Elérhető szimbólumok listája |
| GET | `/snapshots/{ticker}` | Elérhető snapshot dátumok |
| GET | `/vol-surface/{ticker}` | Teljes IV felület (minden lejárat, minden strike) |
| GET | `/term-structure/{ticker}` | ATM IV lejárati bucket szerint |
| GET | `/vol-smile/{ticker}/{expiry}` | Vol smile egy adott lejáratra |

Opcionális query paraméter: `?snapshot_date=ÉÉÉÉ-HH-NN` (alapértelmezett: mai nap)

---

## Pipeline DAG

Az `options_vol_surface_daily` DAG hétköznaponként 18:00 UTC-kor fut automatikusan:

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

Minden task idempotens — ugyanazon dátumra való újrafuttatás törlés + újrainzertálással kezeli a duplikációt, nem keletkeznek ismétlődő sorok.

---

## Adatmodell

```
fact_options_snapshot
  snapshot_id       BIGINT PK
  ticker_id         INT FK → dim_ticker
  expiry_id         INT FK → dim_expiry
  strike_id         INT FK → dim_strike
  option_type       VARCHAR
  implied_vol       DOUBLE
  last_price        DOUBLE
  mid_price         DOUBLE
  bid / ask         DOUBLE
  volume            INTEGER
  open_interest     INTEGER
  underlying_price  DOUBLE
  risk_free_rate    DOUBLE
  snapshot_date     DATE

dim_ticker    — symbol, sector, exchange
dim_expiry    — expiry_date, days_to_expiry, expiry_bucket
dim_strike    — strike_price, moneyness_bucket
```

---

## Analitikai lekérdezések

A `queries/` mappában dokumentált SQL lekérdezések:

- `atm_vol_timeseries.sql` — ATM implied vol idősor tickerenként
- `vol_smile.sql` — IV strike függvényében egy adott lejáratra
- `cross_ticker_comparison.sql` — 30 napos ATM IV összehasonlítás
- `term_structure.sql` — IV lejárati bucket szerint egy adott napra

---

## Környezeti változók

| Változó | Alapértelmezett | Leírás |
|---|---|---|
| `MINIO_ACCESS_KEY` | minioadmin | MinIO felhasználónév |
| `MINIO_SECRET_KEY` | minioadmin | MinIO jelszó |
| `DUCKDB_PATH` | /data/duckdb/vol_surface.db | DuckDB fájl elérési útja |
| `FRED_API_KEY` | — | FRED API kulcs (opcionális) |
| `RISK_FREE_RATE_FALLBACK` | 0.045 | Fallback kamatláb FRED nélkül |
| `TARGET_TICKERS` | SPY | Vesszővel elválasztott szimbólumok |
| `AIRFLOW_USER` | admin | Airflow felhasználónév |
| `AIRFLOW_PASSWORD` | admin | Airflow jelszó |
| `GRAFANA_USER` | admin | Grafana felhasználónév |
| `GRAFANA_PASSWORD` | admin | Grafana jelszó |

---

## Projekt struktúra

```
vol-surface-pipeline/
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.fastapi
├── airflow-entrypoint.sh
├── .env.example
├── README.md
├── docs/
│   └── architecture.md
├── airflow/
│   └── dags/
│       └── options_pipeline_dag.py
├── pipeline/
│   ├── ingestion/
│   │   ├── fetch_options.py
│   │   └── fetch_macro.py
│   ├── storage/
│   │   └── minio_client.py
│   ├── transform/
│   │   ├── clean.py
│   │   ├── iv_surface.py
│   │   └── load_duckdb.py
│   ├── serving/
│   │   └── api.py
│   └── utils/
│       └── config.py
├── grafana/
│   ├── dashboards/
│   └── datasources/
├── queries/
│   ├── atm_vol_timeseries.sql
│   ├── vol_smile.sql
│   ├── cross_ticker_comparison.sql
│   └── term_structure.sql
└── tests/
    └── test_iv_surface.py
```

---

## Leállítás

```bash
docker compose down
```

Teljes reset (adatok törlésével együtt):

```bash
docker compose down -v
```

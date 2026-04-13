# Equity Derivatives IV Surface Pipeline

Napi batch pipeline részvényopciók implied volatility felületének számítására. Black-Scholes inverziót (Brent-módszer) alkalmaz, csillag sémájú DuckDB adattárházba tölt, és REST API-n + Grafana dashboardon keresztül szolgálja ki az eredményt.

**Stack:** yfinance · FRED API · MinIO · DuckDB · Airflow · FastAPI · Grafana · Docker Compose

→ Részletes architektúra, adatmodell és tervezési döntések: [`docs/architecture.md`](docs/architecture.md)

---

## Tartalom

- [Indítás](#indítás)
- [Szolgáltatások](#szolgáltatások)
- [API végpontok](#api-végpontok)
- [Adatmodell](#adatmodell)
- [Projekt struktúra](#projekt-struktúra)

---

## Indítás

**Előfeltétel:** Docker és Docker Compose v2 — egyéb függőség nem szükséges.

```bash
# 1. Klónozás
git clone git@github.com:kadam-x/vol-surface-pipeline.git
cd vol-surface-pipeline

# 2. Környezeti változók
cp .env.example .env
# FRED API kulcs opcionális — nélküle fallback kamatlábbal fut

# 3. Build és indítás
docker compose build && docker compose up -d

# 4. Várakozás az inicializálásra (~20-30 mp)
docker compose logs airflow -f
# A "Scheduler started" üzenetig kell várni
```

**Pipeline futtatása** — Airflow UI-ban (`http://localhost:8080`): `options_vol_surface_daily` DAG → toggle be → ▶ Trigger, vagy:

```bash
docker exec -it vol-surface-airflow airflow dags trigger options_vol_surface_daily
```

**Adatok ellenőrzése:**

```bash
docker exec -it vol-surface-airflow python3 -c "
import duckdb
conn = duckdb.connect('/data/duckdb/vol_surface.db', read_only=True)
print('sorok:', conn.execute('SELECT COUNT(*) FROM fact_options_snapshot').fetchone()[0])
print('szimbólumok:', conn.execute('SELECT symbol FROM dim_ticker').fetchall())
"
```

```bash
curl http://localhost:8000/health
curl http://localhost:8000/vol-surface/SPY
```

---

## Szolgáltatások

| Szolgáltatás | URL | Belépés |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| FastAPI | http://localhost:8000/docs | — |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |

---

## API végpontok

| Végpont | Leírás |
|---|---|
| `GET /health` | Service és DB állapot |
| `GET /tickers` | Elérhető szimbólumok |
| `GET /vol-surface/{ticker}` | Teljes IV felület |
| `GET /term-structure/{ticker}` | ATM IV lejárat szerint |
| `GET /vol-smile/{ticker}/{expiry}` | Vol smile egy lejáratra |
| `GET /snapshots/{ticker}` | Elérhető snapshot dátumok |

Opcionális query paraméter: `?snapshot_date=ÉÉÉÉ-HH-NN`

---

## Adatmodell

Csillag séma: egy ténytábla (`fact_options_snapshot`) és három dimenziótábla.

```
dim_ticker ──┐
             ├──► fact_options_snapshot ◄── dim_expiry
dim_strike ──┘
```

Részletes séma, tervezési döntések és ER ábra: [`docs/architecture.md § 2. Adatmodell`](docs/architecture.md)

---

## Projekt struktúra

```
vol-surface-pipeline/
├── docker-compose.yml
├── .env.example
├── docs/
│   └── architecture.md        # Architektúra, adatmodell, tervezési döntések
├── airflow/dags/
│   └── options_pipeline_dag.py
├── pipeline/
│   ├── ingestion/             # fetch_options.py, fetch_macro.py
│   ├── storage/               # minio_client.py
│   ├── transform/             # clean.py, iv_surface.py, load_duckdb.py
│   ├── serving/               # api.py
│   └── utils/                 # config.py
├── grafana/
├── queries/                   # 4 analitikai SQL lekérdezés
└── tests/
    └── test_iv_surface.py
```

---

## Leállítás

```bash
docker compose down        # adatok megmaradnak
docker compose down -v     # teljes reset
```

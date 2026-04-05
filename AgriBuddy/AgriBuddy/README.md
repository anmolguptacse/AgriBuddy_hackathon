# 🌾 AgriBuddy (CropPulse)

> **"Should I sell my crop today or wait?"**
> AgriBuddy answers this in 9 Indian languages — backed by live mandi prices, weather forecasts, and ICAR government advisories — all running on Databricks.

Built for **Bharat Bricks Hacks 2026** | Track: **Swatantra (Indic AI)**

---

## What It Does

Indian farmers lose ₹40K–80K per season due to poor sell-timing. To solve this, we
envisioned AgriBuddy: an intelligent assistant designed to help farmers time the market.
The core idea was to build a robust, Databricks-powered ecosystem to process and deliver
insights. We use Auto Loader to ingest real-time APMC mandi prices into a Delta Lake
medallion pipeline, training Prophet forecasts tracked in MLflow. By cross-referencing
these forecasts with weather data and ICAR advisories via FAISS RAG, the system
evaluates trends to produce clear "SELL or HOLD" verdicts. Sarvam Mayura translates
them into 9 regional languages, and Genie powers the entire conversational experience,
allowing farmers to ask complex questions about the market in plain, natural language.
---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         AGRIBUDDY — SYSTEM ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────────────────┘

  DATA SOURCES
  ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
  │  data.gov.in      │   │  ICAR Advisory    │   │  OpenWeatherMap   │
  │  Mandi CSVs       │   │  PDFs             │   │  API (3-day       │
  │  (APMC prices)    │   │  (post-harvest,   │   │  rainfall)        │
  │                   │   │   pest mgmt)      │   │                   │
  └────────┬──────────┘   └────────┬──────────┘   └────────┬──────────┘
           │                       │                        │
           ▼                       ▼                        │
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                           DATABRICKS WORKSPACE                                       │
│                                                                                      │
│  ┌────────────────────────────────────────────────────────────────────────────────┐  │
│  │                    Unity Catalog: main.croppulse                               │  │
│  │    Governance · Access control · Lineage · Volume storage                      │  │
│  └────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                      │
│  ┌────────────────────┐                                                              │
│  │  Auto Loader       │     DELTA LAKE MEDALLION ARCHITECTURE                        │
│  │  (cloudFiles)      │     ┌──────────┐   ┌──────────┐   ┌──────────────────┐      │
│  │                    │────▶│  Bronze   │──▶│  Silver  │──▶│      Gold        │      │
│  │  • Stream CSVs     │     │ Raw rows  │   │ Cleaned  │   │ avg_7d, avg_30d  │      │
│  │  • Schema evolve   │     │ from APMC │   │ deduped  │   │ pct_change_7d    │      │
│  │  • Checkpoint      │     │ 360 rows  │   │ filtered │   │ trend label      │      │
│  └────────────────────┘     └──────────┘   └──────────┘   └────────┬─────────┘      │
│                                                                     │                │
│                              ┌───────────────────────────────────────┤                │
│                              │                                       │                │
│                              ▼                                       ▼                │
│  ┌────────────────────────────────────────┐   ┌────────────────────────────────────┐ │
│  │         MLflow Experiment              │   │       Prophet Forecasting          │ │
│  │   /Shared/croppulse_prophet            │   │                                    │ │
│  │                                        │   │  • Train on 60 days per crop/mandi │ │
│  │   • 4 runs (Onion×Nashik, Onion×Pune,  │   │  • 10-day price forecast           │ │
│  │     Tomato×Nashik, Tomato×Pune)        │   │  • Multiplicative seasonality       │ │
│  │   • Params: changepoint_prior=0.05     │   │  • Models saved to UC Volume        │ │
│  │   • Metrics: MAPE, MAE                 │   │                                    │ │
│  │   • Model Registry → Production        │   │  Models: /Volumes/main/croppulse/  │ │
│  │                                        │   │    croppulse_vol/models/            │ │
│  └────────────────────────────────────────┘   └────────────────────────────────────┘ │
│                                                                                      │
│  ┌────────────────────────────────────────────────────────────────────────────────┐  │
│  │                        RAG Pipeline (ICAR Advisories)                          │  │
│  │                                                                                │  │
│  │  ICAR PDFs ──▶ PyMuPDF ──▶ 300-word chunks ──▶ MiniLM-L6-v2 ──▶ FAISS index  │  │
│  │                            (icar_chunks table)   embeddings      (IndexFlatIP) │  │
│  │                                                                                │  │
│  │  At query time: embed query → FAISS top-k → format context → LLM prompt       │  │
│  └────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                      │
│  ┌──────────────────────────────┐   ┌─────────────────────────────────────────────┐ │
│  │  Databricks AI Gateway      │   │  Genie Conversation API                     │ │
│  │  (Sarvam      )             │   │  Space: Mandi Prices Q&A                    │ │
│  │                              │   │                                              │ │
│  │  • System prompt: CropPulse │   │  • Natural language → SQL on Delta tables    │ │
│  │  • Grounded recommendations │   │  • Returns columns + rows + SQL              │ │
│  │  • <100 word farmer advice  │   │  • Chat panel in the app UI                  │ │
│  └──────────────────────────────┘   └─────────────────────────────────────────────┘ │
│                                                                                      │
│  ┌────────────────────────────────────────────────────────────────────────────────┐  │
│  │                     Databricks Apps (app.yaml)                                 │  │
│  │    FastAPI + Uvicorn · command: ["python", "app/main.py"]                      │  │
│  │    Secrets loaded via Databricks SDK at runtime                                │  │
│  └────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
           │                        │                         │
           ▼                        ▼                         ▼
  ┌─────────────────┐   ┌────────────────────┐   ┌────────────────────────┐
  │  Decision Engine│   │  Sarvam Mayura     │   │  OpenWeatherMap        │
  │                 │   │  Translation API   │   │  3-day rainfall        │
  │  Price trend    │   │                    │   │  forecast              │
  │  + Weather      │   │  9 Indian langs:   │   │                        │
  │  → SELL / HOLD  │   │  Hindi, Marathi,   │   │  Signal: WET / DRY     │
  │    verdict      │   │  Telugu, Tamil,    │   │  Threshold: 5mm        │
  │                 │   │  Kannada, Gujarati,│   │                        │
  │  Rules:         │   │  Bengali, Punjabi  │   │  Mandi coords:         │
  │  FALLING+WET    │   │                    │   │  Nashik: 19.99°N       │
  │   = SELL NOW    │   │  Protected terms:  │   │  Pune:   18.52°N       │
  │  RISING+DRY     │   │  ₹, APMC, MSP,    │   │                        │
  │   = HOLD        │   │  quintal           │   │                        │
  └─────────────────┘   └────────────────────┘   └────────────────────────┘
           │                        │
           ▼                        ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                     FastAPI Web Application                        │
  │                                                                    │
  │  GET  /              → HTML UI (crop/mandi/language selector)      │
  │  GET  /advisory      → JSON (decision + chart + recommendation)   │
  │  POST /chat/ask      → Genie natural-language query on Delta data  │
  │  GET  /health        → Health check                                │
  │                                                                    │
  │  ┌──────────────────┐  ┌───────────────────┐  ┌────────────────┐  │
  │  │  Advisory Panel  │  │  Price Chart       │  │  Genie Chat    │  │
  │  │  SELL/HOLD badge │  │  10-day actual     │  │  "What are avg │  │
  │  │  Confidence      │  │  10-day forecast   │  │   onion prices │  │
  │  │  ICAR advisory   │  │  Mandi comparison  │  │   in Pune?"    │  │
  │  │  LLM recommend.  │  │  (matplotlib)      │  │  → SQL + table │  │
  │  └──────────────────┘  └───────────────────┘  └────────────────┘  │
  └────────────────────────────────────────────────────────────────────┘
```

### Databricks Components Used

| Component | How It's Used in AgriBuddy |
|---|---|
| **Auto Loader** | Streams mandi CSV files from UC Volume into `bronze_mandi_prices` Delta table with schema evolution and checkpointing |
| **Delta Lake (Medallion)** | Bronze (raw APMC rows) → Silver (cleaned, deduped, filtered) → Gold (7d/30d rolling averages, % change, trend labels) |
| **Unity Catalog** | `main.croppulse` schema governs all tables, volumes, and access policies |
| **MLflow** | Tracks Prophet experiment runs with params (crop, mandi, changepoint_prior), metrics (MAPE, MAE), and model artifacts |
| **MLflow Model Registry** | Promotes best Prophet model to Production stage for app inference |
| **Databricks AI Gateway** | Serves Llama 4 Maverick for grounded, farmer-friendly LLM recommendations |
| **Genie Conversation API** | Powers the chat panel — converts natural language questions into SQL on Delta tables and returns structured results |
| **Databricks Apps** | Hosts the FastAPI application via `app.yaml` with OAuth-based secret management |
| **Databricks SQL Connector** | App queries Silver/Gold Delta tables at runtime for live prices and features |
| **UC Volumes** | Stores raw CSVs, FAISS index, pickled Prophet models, and checkpoints |

---

## Repository Structure

```
AgriBuddy/
├── app/
│   └── main.py                          # FastAPI app — UI, /advisory, /chat/ask endpoints
├── src/croppulse/
│   ├── config.py                        # All constants: tables, API keys, thresholds
│   ├── decision_engine.py               # SELL/HOLD rule engine (trend × weather matrix)
│   ├── chart_utils.py                   # Matplotlib price charts (actual + forecast + mandi bars)
│   ├── llm_client.py                    # Databricks AI Gateway client (Llama 4 Maverick)
│   ├── rag_retrieval.py                 # FAISS-based retrieval over ICAR advisory chunks
│   ├── sarvam_client.py                 # Sarvam Mayura translation (9 Indian languages)
│   ├── weather_utils.py                 # OpenWeatherMap 3-day rainfall signal
│   ├── genie_client.py                  # Databricks Genie Conversation API client
│   └── delta_utils.py                   # Spark helpers: read/write Delta tables
├── notebooks/
│   ├── 01_ingestion/
│   │   └── 01_autoloader_bronze.py      # Auto Loader → Bronze Delta
│   ├── 02_transforms/
│   │   ├── 01_bronze_to_silver.py       # Spark transforms: clean, dedupe, normalise
│   │   └── 02_price_features.py         # Window functions: rolling avg, % change, trend
│   ├── 03_ml/
│   │   ├── 01_prophet_training.py       # Prophet training + MLflow logging (4 models)
│   │   ├── 02_backtesting.py            # Predicted vs Actual (days 61–90)
│   │   └── 03_mlflow_register.py        # Promote best model to Production
│   ├── 04_rag/
│   │   ├── 01_pdf_chunking.py           # ICAR PDFs → 300-word chunks → Delta table
│   │   └── 02_embed_and_index.py        # MiniLM-L6-v2 embeddings → FAISS index
│   └── 07_pipeline/
│       └── 01_master_pipeline.py        # Runs all 8 steps end-to-end
├── config/
│   └── language_codes.json              # Sarvam language code mapping
├── data/
│   └── crop_mandi_map.json              # Mandi coordinates, crop metadata
├── tests/
│   ├── test_decision_engine.py
│   ├── test_rag_retrieval.py
│   └── test_translation.py
├── app.yaml                             # Databricks Apps deployment config
├── requirements.txt                     # Python dependencies (FastAPI runtime)
├── pyproject.toml                       # Package metadata + dev dependencies
└── README.md
```

---

## How to Run

### Prerequisites

- Databricks workspace (AWS/Azure/GCP) with Unity Catalog enabled
- Databricks CLI installed and authenticated
- Python 3.10+
- API keys: Sarvam Mayura, OpenWeatherMap

### Step 1 — Clone the Repository

```bash
git clone https://github.com/<your-org>/AgriBuddy.git
cd AgriBuddy
```

### Step 2 — Store Secrets in Databricks

```bash
databricks secrets create-scope croppulse
databricks secrets put-secret croppulse sarvam_api_key
databricks secrets put-secret croppulse openweather_api_key
```

### Step 3 — Run the Master Pipeline (One Notebook)

Import the repo into your Databricks workspace, then open and run:

```
notebooks/07_pipeline/01_master_pipeline.py
```

This executes all 8 steps sequentially:

| Step | Notebook | What It Does |
|------|----------|-------------|
| 1/8 | `01_autoloader_bronze.py` | Generates 90 days × 4 crop-mandi combos → writes `bronze_mandi_prices` |
| 2/8 | `01_bronze_to_silver.py` | Cleans, normalises crop names, deduplicates → `silver_mandi_prices` |
| 3/8 | `02_price_features.py` | Spark window functions: 7d/30d rolling avg, % change → `gold_price_features` |
| 4/8 | `01_prophet_training.py` | Trains 4 Prophet models, logs to MLflow, saves to UC Volume |
| 5/8 | `02_backtesting.py` | Evaluates on days 61–90, produces Predicted vs Actual chart |
| 6/8 | `03_mlflow_register.py` | Promotes best model to MLflow Model Registry (Production) |
| 7/8 | `01_pdf_chunking.py` | Chunks ICAR advisory text → `icar_chunks` Delta table |
| 8/8 | `02_embed_and_index.py` | Embeds chunks with MiniLM-L6-v2 → builds FAISS index |

Expected runtime: ~15–20 minutes on a single-node cluster.

### Step 4 — Launch the App Locally

```bash
pip install -r requirements.txt
python app/main.py
```

App starts at **http://localhost:8000**.

### Step 5 — Deploy on Databricks Apps

```bash
# In the Databricks workspace:
# Compute → Apps → Create App → connect this repo → Deploy
# Uses app.yaml as entry point
```

### Step 6 — Run Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Demo Steps

### Demo 1 — Get a SELL/HOLD Advisory

1. Open the app at **http://localhost:8000** (or your Databricks Apps URL)
2. Set the dropdowns: **Crop** = `Onion`, **Mandi** = `Nashik`, **Language** = `Marathi`
3. Click **"Get Advisory"**
4. You will see:
   - A red **SELL SOON** badge (because Nashik onion prices are falling at -12.8% over 7 days)
   - Confidence level: **MEDIUM**
   - Today's price: **₹820/quintal**, 7-day change: **-12.8%**, trend: **FALLING**
   - Weather signal: **DRY — 0.0 mm**
   - ICAR advisory text (translated to Marathi)
   - A two-panel chart: 10-day actual prices (solid green) + 10-day forecast (dashed red) + mandi comparison bars
   - LLM-generated recommendation in Marathi

### Demo 2 — Try a Different Crop + Language

1. Change dropdowns to: **Crop** = `Tomato`, **Mandi** = `Pune`, **Language** = `Hindi`
2. Click **"Get Advisory"**
3. You will see a **SELL SOON** badge (Pune tomato prices falling at -6.3%) with the recommendation translated to Hindi

### Demo 3 — Ask the Genie Chat

1. Click the **"💬 Ask Questions About Mandi Prices"** panel at the bottom to expand it
2. Type: `What are the average onion prices in Nashik?`
3. Press **Enter** or click **Send**
4. Genie converts your question to SQL, runs it on the Delta tables, and returns a structured table with columns and data
5. Click **"Show SQL"** to see the generated query

### Demo 4 — Explore MLflow

1. In the Databricks workspace, navigate to **Machine Learning → Experiments → /Shared/croppulse_prophet**
2. View 4 training runs (Onion×Nashik, Onion×Pune, Tomato×Nashik, Tomato×Pune)
3. Compare MAPE and MAE metrics across runs
4. Click into the best run to see parameters, metrics, and the model artifact path
5. Navigate to **Models → croppulse-prophet** to see the Production model version

### Demo 5 — Inspect Delta Tables in Unity Catalog

1. Go to **Catalog → main → croppulse**
2. Browse the four Delta tables:
   - `bronze_mandi_prices` — raw ingested rows (360 records)
   - `silver_mandi_prices` — cleaned and deduplicated
   - `gold_price_features` — with `avg_7d`, `avg_30d`, `pct_change_7d`, `trend`
   - `icar_chunks` — ICAR advisory text chunks for RAG
3. Click **Sample Data** on `gold_price_features` to see the computed features

---

## Decision Logic

The decision engine applies a deterministic rule table combining price trend and weather:

| Price Trend | Weather | Decision | Confidence |
|---|---|---|---|
| FALLING | WET | **SELL NOW** | HIGH |
| FALLING | DRY | **SELL SOON** | MEDIUM |
| RISING | DRY | **HOLD** | HIGH |
| RISING | WET | **HOLD SHORT** | MEDIUM |
| STABLE | any | **MONITOR** | LOW |

Thresholds: price fall < -5% = FALLING, price rise > +5% = RISING, rain ≥ 5mm = WET.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data ingestion | Databricks Auto Loader (cloudFiles) |
| Data storage | Delta Lake (Bronze / Silver / Gold), Unity Catalog |
| Feature engineering | PySpark window functions (rolling avg, % change) |
| Forecasting | Prophet (multiplicative seasonality, weekly) |
| Experiment tracking | MLflow (params, metrics, model registry) |
| LLM | Databricks AI Gateway — Llama 4 Maverick |
| RAG embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Vector search | FAISS (IndexFlatIP, cosine similarity) |
| Translation | Sarvam Mayura API (9 Indian languages) |
| Weather | OpenWeatherMap 5-day/3-hour forecast API |
| NL-to-SQL | Databricks Genie Conversation API |
| App framework | FastAPI + Uvicorn on Databricks Apps |

---

## Delta Tables Created

| Table | Description |
|---|---|
| `main.croppulse.bronze_mandi_prices` | Raw APMC mandi price rows (date, crop, mandi, modal/min/max price, arrivals) |
| `main.croppulse.silver_mandi_prices` | Cleaned, deduplicated, normalised crop names, partitioned by crop+mandi |
| `main.croppulse.gold_price_features` | Rolling averages, % change, trend labels — consumed by decision engine |
| `main.croppulse.icar_chunks` | 300-word chunks from ICAR advisory PDFs, tagged by crop and source |

---

## Team

Built at **Bharat Bricks Hacks 2026** — IISc Bengaluru

---

## License

This project is released under the [MIT License](LICENSE).

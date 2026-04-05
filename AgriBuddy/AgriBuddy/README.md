# CropPulse — AI Advisor for Indian Farmers

> **"Should I sell my crop today or wait?"**
> CropPulse answers this in 9 Indian languages — backed by live mandi prices, weather forecasts, and ICAR government advisories — all running on Databricks.

Built for **Bharat Bricks Hacks 2026** | Track: **Swatantra (Indic AI)**

---

## What it does

Indian smallholder farmers lose ₹40,000–80,000 per season not from bad harvests, but from a single bad timing decision. CropPulse gives them a real-time, cited SELL or HOLD recommendation by combining:

| Signal | Source | Databricks component |
|--------|--------|---------------------|
| Past 10-day price trend | APMC mandi data (data.gov.in) | Autoloader → Delta Bronze/Silver/Gold |
| 10-day price forecast | Prophet model | MLflow |
| Today's price across all mandis | Silver Delta table | Spark SQL |
| 3-day weather forecast | OpenWeatherMap API | Python |
| Government advisory (cited) | ICAR PDFs via FAISS RAG | sentence-transformers + FAISS |
| Output language | 9 Indian languages | Sarvam Mayura API |

---

## Architecture

```
data.gov.in CSVs
      │
      ▼
 Autoloader ──► Bronze Delta ──► Spark transforms ──► Silver Delta ──► Gold Delta (features)
                                                                              │
                                                                              ▼
ICAR PDFs ──► PyMuPDF chunks ──► MiniLM-L6 embeddings ──► FAISS index    Prophet + MLflow
      │                                                        │               │
      └────────────────────── RAG retrieval ◄──────────────────┘               │
                                     │                                         │
                              LLM prompt builder ◄── OpenWeatherMap            │
                              (Llama 4 Maverick)  ◄─────────────────── forecast│
                                     │
                              Sarvam Mayura (translate)
                                     │
                              Gradio UI (Databricks App)
```

---

## Databricks components used

| Component | Usage |
|-----------|-------|
| **Autoloader** | Streams mandi CSVs → Bronze Delta |
| **Delta Lake** | Bronze / Silver / Gold tables + chunk table |
| **MLflow** | Prophet experiment tracking, model registry |
| **AI Gateway** | Llama 4 Maverick for grounded recommendations |
| **Databricks Apps** | Hosts the Gradio UI |

---

## Demo

**Query:** Crop = Onion, Mandi = Nashik, Language = Marathi

**Output:**
```
SELL NOW (HIGH confidence)
आत्ता विका — Nashik मध्ये कांद्याची किंमत गेल्या ७ दिवसांत १२% घसरली आहे.
पुढील १० दिवसांत आणखी ८% घट अपेक्षित आहे.

Advisory: "Kharif onion should be sold within 10 days when humidity > 75%"
Source: onion_postharvest_advisory.pdf | Page 4
```

---

## How to run (judges — exact steps)

### 1. Prerequisites
```bash
git clone https://github.com/your-team/croppulse
cd croppulse
cp .env.example .env   # fill in your keys
```

### 2. Store secrets in Databricks
```bash
databricks secrets create-scope croppulse
databricks secrets put-secret croppulse sarvam_api_key
databricks secrets put-secret croppulse openweather_api_key
databricks secrets put-secret croppulse databricks_token
```

### 3. Run the master pipeline (one notebook)
In Databricks, open and run:
```
notebooks/07_pipeline/01_master_pipeline.py
```
This runs all 8 steps: ingestion → transforms → ML → RAG indexing.
Expected runtime: ~15–20 minutes on a single-node cluster.

### 4. Launch the app locally
```bash
pip install -e ".[dev]"
python app/main.py
# Opens at http://localhost:7860
```

### 5. Deploy on Databricks Apps
```
Compute → Apps → Create App → connect this repo → Deploy
```
Uses `app.yaml` as entry point.

---

## Run tests
```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Tech stack

| Layer | Technology |
|-------|-----------|
| LLM | Databricks AI Gateway — Llama 4 Maverick |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Vector search | FAISS (IndexFlatIP, cosine similarity) |
| Translation | Sarvam Mayura API |
| Forecasting | Prophet + MLflow |
| App framework | Gradio 4.44 on Databricks Apps |
| Data platform | Databricks Delta Lake, Autoloader, Unity Catalog |

---

## Team
Built at Bharat Bricks Hacks 2026 — IISc Bengaluru

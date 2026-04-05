import os

# ── Databricks workspace ──────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# ── Unity Catalog ─────────────────────────────────────────────────────────────
CATALOG = "main"
SCHEMA  = "croppulse"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_mandi_prices"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_mandi_prices"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.gold_price_features"
CHUNK_TABLE  = f"{CATALOG}.{SCHEMA}.icar_chunks"

# UC Volume — stores FAISS index
VOLUME_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/croppulse_vol"
FAISS_INDEX_PATH = f"{VOLUME_PATH}/faiss_index"

# ── Raw file paths (DBFS) ─────────────────────────────────────────────────────
RAW_DATA_PATH = "/Volumes/main/croppulse/croppulse_vol/raw/"
ICAR_PDF_PATH = "/Volumes/main/croppulse/croppulse_vol/icar_pdfs/"

# ── MLflow ────────────────────────────────────────────────────────────────────
MLFLOW_EXPERIMENT_NAME = "/Shared/croppulse_prophet"
MODEL_REGISTRY_NAME    = "croppulse-prophet"
FORECAST_PERIODS       = 10   # days ahead to predict

# ── Crops & Mandis ────────────────────────────────────────────────────────────
CROPS  = ["Onion", "Tomato"]
MANDIS = ["Nashik", "Pune"]

MANDI_COORDS = {
    "Nashik": {"lat": 19.9975, "lon": 73.7898},
    "Pune":   {"lat": 18.5204, "lon": 73.8567},
}

CROP_NAME_MAP = {
    # Hindi / Marathi aliases → canonical English
    "pyaaz": "Onion",   "kanda": "Onion",   "vengayam": "Onion",
    "tamatar": "Tomato", "thakkali": "Tomato",
}

# ── External APIs ─────────────────────────────────────────────────────────────
# OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
# SARVAM_API_KEY      = os.getenv("SARVAM_API_KEY", "") 

SARVAM_API_KEY    = "sk_lhzyltg6_dpeWGbnPBLnoJ7qguQ8KNit"
OPENWEATHER_API_KEY = "fd443cd23e6fbf0408690ac4546813b6"

# ── Genie API ─────────────────────────────────────────────────────────────────
GENIE_SPACE_ID = "01f130b48c0c1086899aaf471ccda926"  # Mandi Prices Q&A space

# Databricks AI Gateway endpoint (Llama 4 Maverick)
AI_GATEWAY_ENDPOINT = os.getenv(
    "DATABRICKS_AI_GATEWAY_ENDPOINT",
    "databricks-meta-llama-3-3-70b-instruct",
)

# ── Language codes (Sarvam Mayura format) ─────────────────────────────────────
LANGUAGE_CODES = {
    "English":  "en-IN",
    "Hindi":    "hi-IN",
    "Marathi":  "mr-IN",
    "Telugu":   "te-IN",
    "Tamil":    "ta-IN",
    "Kannada":  "kn-IN",
    "Gujarati": "gu-IN",
    "Bengali":  "bn-IN",
    "Punjabi":  "pa-IN",
}

SUPPORTED_LANGUAGES = list(LANGUAGE_CODES.keys())

# ── RAG ───────────────────────────────────────────────────────────────────────
CHUNK_SIZE    = 300   # words per chunk
CHUNK_OVERLAP = 50    # words overlap between chunks
TOP_K_CHUNKS  = 2     # number of advisory passages to retrieve

# ── Decision rule thresholds ──────────────────────────────────────────────────
RAIN_THRESHOLD_MM     = 5.0   # mm — above this → WET signal
PRICE_FALL_THRESHOLD  = -5.0  # % 7-day change → FALLING
PRICE_RISE_THRESHOLD  = 5.0   # % 7-day change → RISING

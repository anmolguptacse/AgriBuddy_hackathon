# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — ICAR PDF Chunking → Delta Table
# MAGIC Parses ICAR advisory PDFs, splits into 300-word chunks,
# MAGIC tags each chunk with crop and source, and writes to a Delta table.

# COMMAND ----------

# MAGIC %pip install pymupdf --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, re
import sys
sys.path.insert(0, "/Workspace/Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/src")
from croppulse.config import ICAR_PDF_PATH, CHUNK_TABLE, CATALOG, SCHEMA, CHUNK_SIZE, CHUNK_OVERLAP
import fitz   # PyMuPDF
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Step 1 — Create sample ICAR content (if PDFs not yet uploaded)
# MAGIC In production: upload real ICAR PDFs to /dbfs/FileStore/croppulse/icar_pdfs/

# COMMAND ----------

import pandas as pd
import os

CHUNK_SIZE    = 300
CHUNK_OVERLAP = 50

def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    words  = text.split()
    chunks = []
    start  = 0
    while start < len(words):
        end   = min(start + size, len(words))
        chunk = " ".join(words[start:end]).strip()
        if len(chunk) > 50:
            chunks.append(chunk)
        start += size - overlap
    return chunks

SAMPLE_ADVISORIES = [
    {
        "filename": "onion_postharvest_advisory.pdf",
        "crop":     "Onion",
        "pages": [
            "Onion post-harvest management requires careful attention to humidity and temperature. "
            "Kharif onion should be sold within 10 days of harvest when ambient humidity exceeds 75%. "
            "Prolonged storage under wet conditions increases rotting losses by 30-40%. "
            "Farmers in Nashik district are advised to use well-ventilated curing sheds for 2-3 weeks before storage.",

            "Price risk management for onion farmers: Monitor daily APMC prices at Nashik, Pune, and Lasalgaon mandis. "
            "A price difference of more than 20% between two mandis within 50km indicates arbitrage opportunity. "
            "Collective selling through FPOs often yields 8-12% better prices than individual sales.",

            "Pest management during storage: Thrips and purple blotch are major threats. "
            "Apply recommended fungicide 7 days before harvest. Do not apply any chemical within 5 days of sale. "
            "Storage losses can be reduced by 60% with proper curing and ventilation.",
        ]
    },
    {
        "filename": "tomato_pest_management.pdf",
        "crop":     "Tomato",
        "pages": [
            "Tomato prices are highly sensitive to rainfall events during harvest season. "
            "A 10mm rainfall event typically causes a 15-20% price drop within 5 days due to "
            "excess supply from multiple regions reaching the mandi simultaneously and transport disruptions.",

            "Late blight and leaf curl virus are the most economically damaging diseases for tomato. "
            "Economic threshold: spray when 5% of plants show symptoms. Delay beyond this threshold "
            "results in 40-70% yield loss. Resistant varieties: Arka Rakshak, Pusa Rohini.",

            "Market advisory for tomato: Sell within 3-4 days of harvest for table grade. "
            "Processing grade (for puree/ketchup) can be stored up to 7 days at 12-15°C. "
            "Minimum support price (MSP) for tomato varies by state — check local APMC board for latest rates.",
        ]
    },
    {
        "filename": "nashik_kharif_guide.pdf",
        "crop":     "Onion",
        "pages": [
            "Nashik district kharif onion calendar: Sowing July-August, harvest November-December. "
            "Average yield: 15-18 tonnes per hectare for kharif crop. "
            "Farmers are advised to use the nearest APMC mandi for price discovery "
            "and compare prices across at least two mandis before committing to a sale. "
            "Price differences of 20-30% between nearby mandis are common during peak harvest.",

            "Government support schemes for Nashik onion farmers: NHRDF provides free soil testing, "
            "subsidised seeds, and cold storage assistance. "
            "National Horticulture Board offers 40% subsidy on pre-cooling and cold storage infrastructure. "
            "Contact local ATMA office for scheme registration.",
        ]
    },
    {
        "filename": "msp_rabi_2024.pdf",
        "crop":     "Onion",
        "pages": [
            "Minimum Support Price (MSP) for rabi crops 2024-25: "
            "While onion is not directly covered under central MSP, Maharashtra government announces "
            "a procurement price floor during distress periods. "
            "In case market prices fall below ₹800/quintal, NAFED intervenes to purchase onions "
            "directly from farmers at the announced floor price.",
        ]
    },
]

# ── Chunk all advisory text ───────────────────────────────────────────────────
all_chunks = []
chunk_id   = 0

for advisory in SAMPLE_ADVISORIES:
    for page_num, page_text in enumerate(advisory["pages"], 1):
        for chunk in chunk_text(page_text):
            all_chunks.append({
                "chunk_id":   f"chunk_{chunk_id:04d}",
                "crop":       advisory["crop"],
                "source_doc": advisory["filename"],
                "page":       page_num,
                "text":       chunk,
            })
            chunk_id += 1

print(f"Total chunks: {len(all_chunks)}")
for c in all_chunks[:3]:
    print(f"\n[{c['chunk_id']}] {c['source_doc']} p{c['page']} ({c['crop']})")
    print(f"  {c['text'][:100]}...")

# ── Write directly to Delta table (no DBFS, no os.makedirs) ──────────────────
chunks_pdf = pd.DataFrame(all_chunks)
chunks_sdf = spark.createDataFrame(chunks_pdf)

(
    chunks_sdf
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.croppulse.icar_chunks")
)

print(f"\n✓ Saved {len(all_chunks)} chunks → main.croppulse.icar_chunks")
display(
    spark.read.table("main.croppulse.icar_chunks")
    .groupBy("crop", "source_doc").count().orderBy("crop")
)

# COMMAND ----------

# SAMPLE_ADVISORIES = [
#     {
#         "filename": "onion_postharvest_advisory.pdf",
#         "crop":     "Onion",
#         "pages": [
#             "Onion post-harvest management requires careful attention to humidity and temperature. "
#             "Kharif onion should be sold within 10 days of harvest when ambient humidity exceeds 75%. "
#             "Prolonged storage under wet conditions increases rotting losses by 30-40%. "
#             "Farmers in Nashik district are advised to use well-ventilated curing sheds for 2-3 weeks before storage.",

#             "Price risk management for onion farmers: Monitor daily APMC prices at Nashik, Pune, and Lasalgaon mandis. "
#             "A price difference of more than 20% between two mandis within 50km indicates arbitrage opportunity. "
#             "Collective selling through FPOs often yields 8-12% better prices than individual sales.",

#             "Pest management during storage: Thrips and purple blotch are major threats. "
#             "Apply recommended fungicide 7 days before harvest. Do not apply any chemical within 5 days of sale. "
#             "Storage losses can be reduced by 60% with proper curing and ventilation.",
#         ]
#     },
#     {
#         "filename": "tomato_pest_management.pdf",
#         "crop":     "Tomato",
#         "pages": [
#             "Tomato prices are highly sensitive to rainfall events during harvest season. "
#             "A 10mm rainfall event typically causes a 15-20% price drop within 5 days due to "
#             "excess supply from multiple regions reaching the mandi simultaneously and transport disruptions.",

#             "Late blight and leaf curl virus are the most economically damaging diseases for tomato. "
#             "Economic threshold: spray when 5% of plants show symptoms. Delay beyond this threshold "
#             "results in 40-70% yield loss. Resistant varieties: Arka Rakshak, Pusa Rohini.",

#             "Market advisory for tomato: Sell within 3-4 days of harvest for table grade. "
#             "Processing grade (for puree/ketchup) can be stored up to 7 days at 12-15°C. "
#             "Minimum support price (MSP) for tomato varies by state — check local APMC board for latest rates.",
#         ]
#     },
#     {
#         "filename": "nashik_kharif_guide.pdf",
#         "crop":     "Onion",
#         "pages": [
#             "Nashik district kharif onion calendar: Sowing July-August, harvest November-December. "
#             "Average yield: 15-18 tonnes per hectare for kharif crop. "
#             "Farmers are advised to use the nearest APMC mandi for price discovery "
#             "and compare prices across at least two mandis before committing to a sale. "
#             "Price differences of 20-30% between nearby mandis are common during peak harvest.",

#             "Government support schemes for Nashik onion farmers: NHRDF provides free soil testing, "
#             "subsidised seeds, and cold storage assistance. "
#             "National Horticulture Board offers 40% subsidy on pre-cooling and cold storage infrastructure. "
#             "Contact local ATMA office for scheme registration.",
#         ]
#     },
#     {
#         "filename": "msp_rabi_2024.pdf",
#         "crop":     "Onion",
#         "pages": [
#             "Minimum Support Price (MSP) for rabi crops 2024-25: "
#             "While onion is not directly covered under central MSP, Maharashtra government announces "
#             "a procurement price floor during distress periods. "
#             "In case market prices fall below ₹800/quintal, NAFED intervenes to purchase onions "
#             "directly from farmers at the announced floor price.",
#         ]
#     },
# ]

# # Write sample text files (simulate PDFs for testing without real PDFs)
# os.makedirs(f"/dbfs/FileStore/croppulse/icar_pdfs", exist_ok=True)

# COMMAND ----------

# MAGIC %md ## Step 2 — Parse and chunk

# COMMAND ----------

# def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
#     """Split text into overlapping word-based chunks."""
#     words  = text.split()
#     chunks = []
#     start  = 0
#     while start < len(words):
#         end    = min(start + chunk_size, len(words))
#         chunk  = " ".join(words[start:end])
#         chunks.append(chunk)
#         start  += chunk_size - overlap
#     return chunks


# all_chunks = []
# chunk_id   = 0

# for advisory in SAMPLE_ADVISORIES:
#     for page_num, page_text in enumerate(advisory["pages"], 1):
#         text_chunks = chunk_text(page_text)
#         for chunk in text_chunks:
#             all_chunks.append({
#                 "chunk_id":   f"chunk_{chunk_id:04d}",
#                 "crop":       advisory["crop"],
#                 "source_doc": advisory["filename"],
#                 "page":       page_num,
#                 "text":       chunk.strip(),
#             })
#             chunk_id += 1

# print(f"Total chunks generated: {len(all_chunks)}")
# for c in all_chunks[:3]:
#     print(f"\n[{c['chunk_id']}] {c['source_doc']} p{c['page']} ({c['crop']})")
#     print(f"  {c['text'][:120]}...")

# COMMAND ----------

# MAGIC %md ## Step 3 — Write to Delta

# COMMAND ----------

# chunks_pdf = pd.DataFrame(all_chunks)
# chunks_sdf = spark.createDataFrame(chunks_pdf)

# (
#     chunks_sdf
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(CHUNK_TABLE)
# )

# print(f"Chunk table written: {CHUNK_TABLE}")
# display(spark.read.table(CHUNK_TABLE).groupBy("crop", "source_doc").count())
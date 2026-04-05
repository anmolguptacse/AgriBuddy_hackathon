# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Embed Chunks + Build FAISS Index
# MAGIC Embeds ICAR chunks using MiniLM-L6-v2 and builds a FAISS index
# MAGIC stored in the UC Volume. Mirrors nyaya-dhwani-hackathon approach.

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys, os, pickle
sys.path.insert(0, "/Workspace/Repos/croppulse/src")

from croppulse.config import CHUNK_TABLE, FAISS_INDEX_PATH
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# COMMAND ----------

# MAGIC %md ## Step 1 — Load chunks from Delta

# COMMAND ----------

chunks_pdf = spark.read.table(CHUNK_TABLE).toPandas()
print(f"Loaded {len(chunks_pdf)} chunks for embedding")
display(chunks_pdf.head(3))

# COMMAND ----------

# MAGIC %md ## Step 2 — Embed using all-MiniLM-L6-v2

# COMMAND ----------

print("Loading SentenceTransformer model…")
model      = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
texts      = chunks_pdf["text"].tolist()

print(f"Embedding {len(texts)} chunks…")
embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=True)
embeddings = embeddings.astype("float32")

print(f"Embeddings shape: {embeddings.shape}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Build FAISS index

# COMMAND ----------

dim   = embeddings.shape[1]
index = faiss.IndexFlatIP(dim)      # Inner product = cosine similarity (since normalised)
index.add(embeddings)

print(f"FAISS index: {index.ntotal} vectors, dim={dim}")

# COMMAND ----------

# MAGIC %md ## Step 4 — Save index + chunks to UC Volume

# COMMAND ----------

os.makedirs(f"/dbfs{FAISS_INDEX_PATH.replace('/dbfs','').replace('/Volumes','')}", exist_ok=True)

# Map Volume path for DBFS access
dbfs_index_path = FAISS_INDEX_PATH.replace(
    "/Volumes/main/croppulse/croppulse_vol",
    "/dbfs/FileStore/croppulse/faiss_index"
)
os.makedirs(dbfs_index_path, exist_ok=True)

faiss.write_index(index, os.path.join(dbfs_index_path, "index.faiss"))

chunks_data = chunks_pdf.to_dict(orient="records")
with open(os.path.join(dbfs_index_path, "chunks.pkl"), "wb") as f:
    pickle.dump(chunks_data, f)

print(f"Saved FAISS index → {dbfs_index_path}/index.faiss")
print(f"Saved chunks     → {dbfs_index_path}/chunks.pkl")

# COMMAND ----------

# MAGIC %md ## Step 5 — Test retrieval

# COMMAND ----------

test_queries = [
    "onion post-harvest storage humidity",
    "tomato price rainfall effect",
    "sell or hold mandi price decision",
]

for query in test_queries:
    q_embed = model.encode([query], normalize_embeddings=True).astype("float32")
    scores, indices = index.search(q_embed, 2)
    print(f"\nQuery: '{query}'")
    for score, idx in zip(scores[0], indices[0]):
        chunk = chunks_data[idx]
        print(f"  [{score:.3f}] {chunk['source_doc']} p{chunk['page']}: {chunk['text'][:100]}…")
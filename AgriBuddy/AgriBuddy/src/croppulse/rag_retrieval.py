"""
rag_retrieval.py — FAISS-based retrieval over ICAR advisory corpus.
Mirrors the pattern used in nyaya-dhwani-hackathon (FAISS + MiniLM).

Index is built by notebooks/04_rag/02_embed_and_index.py
and stored in the UC Volume at config.FAISS_INDEX_PATH.
"""
from __future__ import annotations

import json
import os
import pickle
from pathlib import Path
from typing import Optional

import numpy as np

from croppulse.config import FAISS_INDEX_PATH, TOP_K_CHUNKS

_index = None
_chunks: list[dict] = []


def _load_index() -> None:
    global _index, _chunks
    if _index is not None:
        return

    try:
        import faiss
        index_file  = os.path.join(FAISS_INDEX_PATH, "index.faiss")
        chunks_file = os.path.join(FAISS_INDEX_PATH, "chunks.pkl")
        _index  = faiss.read_index(index_file)
        with open(chunks_file, "rb") as f:
            _chunks = pickle.load(f)
        print(f"[rag_retrieval] Loaded FAISS index — {len(_chunks)} chunks")
    except Exception as exc:
        print(f"[rag_retrieval] FAISS load failed ({exc}), using mock chunks")
        _index  = None
        _chunks = _mock_chunks()


def _embed(texts: list[str]) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    embeddings = model.encode(texts, normalize_embeddings=True)
    return embeddings.astype("float32")


def retrieve(query: str, crop: Optional[str] = None, top_k: int = TOP_K_CHUNKS) -> list[dict]:
    """
    Retrieve top-k ICAR advisory passages for a query.

    Parameters
    ----------
    query : str  — natural language query, e.g. "onion post-harvest storage"
    crop  : str  — optional filter to keep only chunks tagged for this crop
    top_k : int  — number of passages to return

    Returns
    -------
    list of dicts with keys: text, source_doc, page, score
    """
    _load_index()

    if _index is None:
        # Return mock chunks when index unavailable (local dev)
        filtered = [c for c in _chunks if crop is None or c.get("crop", "").lower() == crop.lower()]
        return filtered[:top_k]

    import faiss
    query_vec = _embed([query])
    scores, indices = _index.search(query_vec, top_k * 3)  # over-fetch, then filter

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx < 0 or idx >= len(_chunks):
            continue
        chunk = _chunks[idx]
        if crop and chunk.get("crop", "").lower() != crop.lower():
            continue
        results.append({**chunk, "score": float(score)})
        if len(results) >= top_k:
            break

    return results


def format_context(chunks: list[dict]) -> str:
    """Format retrieved chunks into a context block for the LLM prompt."""
    lines = []
    for i, c in enumerate(chunks, 1):
        lines.append(
            f"[Advisory {i}] Source: {c.get('source_doc','unknown')} | "
            f"Crop: {c.get('crop','?')} | Page: {c.get('page','?')}\n"
            f"{c['text']}"
        )
    return "\n\n".join(lines)


def _mock_chunks() -> list[dict]:
    return [
        {
            "text": (
                "Kharif onion should be sold within 10 days of harvest when "
                "ambient humidity exceeds 75%. Prolonged storage under wet "
                "conditions increases rotting losses by 30-40%."
            ),
            "source_doc": "onion_postharvest_advisory.pdf",
            "page": 4,
            "crop": "Onion",
        },
        {
            "text": (
                "Tomato prices are highly sensitive to rainfall events during "
                "harvest season. A 10mm rainfall event typically causes a 15-20% "
                "price drop within 5 days due to excess supply and transport disruption."
            ),
            "source_doc": "tomato_pest_management.pdf",
            "page": 12,
            "crop": "Tomato",
        },
        {
            "text": (
                "Farmers are advised to use the nearest APMC mandi for price discovery "
                "and compare prices across at least two mandis before committing to a sale. "
                "Price differences of 20-30% between nearby mandis are common."
            ),
            "source_doc": "nashik_kharif_guide.pdf",
            "page": 7,
            "crop": "Onion",
        },
    ]

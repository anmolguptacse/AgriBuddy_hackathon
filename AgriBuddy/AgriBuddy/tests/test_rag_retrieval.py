"""
tests/test_rag_retrieval.py
pytest tests/test_rag_retrieval.py -v
Tests RAG logic using mock chunks (no FAISS index needed).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from unittest.mock import patch
import croppulse.rag_retrieval as rag


def test_retrieve_returns_list():
    """retrieve() should always return a list."""
    with patch.object(rag, "_index", None), \
         patch.object(rag, "_chunks", rag._mock_chunks()):
        results = rag.retrieve("onion storage", crop="Onion")
    assert isinstance(results, list)


def test_retrieve_respects_top_k():
    with patch.object(rag, "_index", None), \
         patch.object(rag, "_chunks", rag._mock_chunks()):
        results = rag.retrieve("onion storage", crop="Onion", top_k=1)
    assert len(results) <= 1


def test_retrieve_filters_by_crop():
    with patch.object(rag, "_index", None), \
         patch.object(rag, "_chunks", rag._mock_chunks()):
        results = rag.retrieve("price storage", crop="Tomato", top_k=5)
    for r in results:
        assert r["crop"].lower() == "tomato"


def test_chunk_has_required_keys():
    with patch.object(rag, "_index", None), \
         patch.object(rag, "_chunks", rag._mock_chunks()):
        results = rag.retrieve("onion", top_k=2)
    for r in results:
        for key in ["text", "source_doc", "page", "crop"]:
            assert key in r, f"Missing key: {key}"


def test_format_context_not_empty():
    chunks = rag._mock_chunks()[:2]
    ctx = rag.format_context(chunks)
    assert len(ctx) > 50
    assert "Advisory 1" in ctx
    assert "Advisory 2" in ctx


def test_format_context_includes_source():
    chunks = rag._mock_chunks()[:1]
    ctx = rag.format_context(chunks)
    assert chunks[0]["source_doc"] in ctx


def test_retrieve_no_crop_filter_returns_all():
    mock = rag._mock_chunks()
    with patch.object(rag, "_index", None), \
         patch.object(rag, "_chunks", mock):
        results = rag.retrieve("harvest", crop=None, top_k=10)
    assert len(results) == len(mock)

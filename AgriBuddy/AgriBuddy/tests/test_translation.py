"""
tests/test_translation.py
pytest tests/test_translation.py -v
Tests translation logic without hitting the real Sarvam API.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from unittest.mock import patch, MagicMock
from croppulse.sarvam_client import (
    translate, _mask_protected, _restore_protected, PROTECTED_TERMS
)


def test_english_passthrough():
    """English input should return unchanged."""
    result = translate("Sell your onions now.", "English")
    assert result == "Sell your onions now."


def test_unsupported_language_passthrough():
    """Unknown language should return original text."""
    result = translate("Some text", "Swahili")
    assert result == "Some text"


def test_mask_protected_terms():
    text = "Sell your onions at Nashik mandi today for ₹820/quintal"
    masked, placeholders = _mask_protected(text)
    assert "Nashik" not in masked
    assert "₹" not in masked
    assert len(placeholders) > 0


def test_restore_protected_terms():
    text = "Sell your onions at Nashik mandi today"
    masked, placeholders = _mask_protected(text)
    restored = _restore_protected(masked, placeholders)
    assert "Nashik" in restored


def test_translate_hindi_success():
    """Mock a successful Sarvam API call for Hindi translation."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "translated_text": "अभी बेच दें। __TERM0__ में कीमत गिर रही है।"
    }
    mock_response.raise_for_status = MagicMock()

    with patch("croppulse.sarvam_client.requests.post", return_value=mock_response):
        result = translate("Sell now. Price is falling in Nashik.", "Hindi")

    assert isinstance(result, str)
    assert len(result) > 0
    # Nashik should be restored
    assert "Nashik" in result


def test_translate_api_failure_returns_original():
    """On API failure, original English text should be returned."""
    with patch("croppulse.sarvam_client.requests.post", side_effect=Exception("timeout")):
        result = translate("Sell now.", "Marathi")
    assert result == "Sell now."


def test_all_supported_languages_have_codes():
    from croppulse.config import LANGUAGE_CODES, SUPPORTED_LANGUAGES
    for lang in SUPPORTED_LANGUAGES:
        assert lang in LANGUAGE_CODES, f"Missing code for: {lang}"

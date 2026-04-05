"""
sarvam_client.py — Sarvam Mayura translation for 9 Indian languages.
Mirrors the Sarvam integration pattern from nyaya-dhwani-hackathon.

Sarvam API docs: https://docs.sarvam.ai
"""
from __future__ import annotations

import requests

from croppulse.config import SARVAM_API_KEY, LANGUAGE_CODES

SARVAM_TRANSLATE_URL = "https://api.sarvam.ai/translate"
SARVAM_TTS_URL       = "https://api.sarvam.ai/text-to-speech"


# Proper nouns that must NOT be translated
PROTECTED_TERMS = [
    "Nashik", "Pune", "Onion", "Tomato", "APMC",
    "ICAR", "MSP", "quintal", "₹",
]


def _mask_protected(text: str) -> tuple[str, dict]:
    """Replace protected terms with placeholders before translation."""
    placeholders = {}
    masked = text
    for i, term in enumerate(PROTECTED_TERMS):
        ph = f"__TERM{i}__"
        if term in masked:
            placeholders[ph] = term
            masked = masked.replace(term, ph)
    return masked, placeholders


def _restore_protected(text: str, placeholders: dict) -> str:
    """Restore placeholders after translation."""
    restored = text
    for ph, term in placeholders.items():
        restored = restored.replace(ph, term)
    return restored


def translate(text: str, target_language: str) -> str:
    """
    Translate text to the target language using Sarvam Mayura.

    Parameters
    ----------
    text            : English text to translate
    target_language : Language name, e.g. "Hindi", "Marathi"

    Returns
    -------
    Translated string (or original English if translation fails / lang=English)
    """
    if target_language == "English" or target_language not in LANGUAGE_CODES:
        return text

    lang_code = LANGUAGE_CODES[target_language]
    masked_text, placeholders = _mask_protected(text)

    headers = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "input":           masked_text,
        "source_language": "en-IN",
        "target_language": lang_code,
        "speaker_gender":  "Male",
        "mode":            "formal",
        "model":           "mayura:v1",
        "enable_preprocessing": True,
    }

    try:
        resp = requests.post(SARVAM_TRANSLATE_URL, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        translated = resp.json().get("translated_text", text)
        return _restore_protected(translated, placeholders)
    except Exception as exc:
        print(f"[sarvam_client] Translation error for {target_language}: {exc}")
        return text  # return original English on failure


def text_to_speech(text: str, language: str) -> bytes | None:
    """
    Convert text to speech using Sarvam Bulbul TTS.
    Returns audio bytes or None on failure.
    """
    lang_code = LANGUAGE_CODES.get(language, "en-IN")
    headers = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "inputs":          [text[:500]],   # Sarvam TTS limit
        "target_language": lang_code,
        "speaker":         "arvind",
        "pitch":           0,
        "pace":            1.0,
        "loudness":        1.5,
        "speech_sample_rate": 8000,
        "enable_preprocessing": True,
        "model": "bulbul:v1",
    }

    try:
        resp = requests.post(SARVAM_TTS_URL, json=payload, headers=headers, timeout=20)
        resp.raise_for_status()
        audios = resp.json().get("audios", [])
        if audios:
            import base64
            return base64.b64decode(audios[0])
        return None
    except Exception as exc:
        print(f"[sarvam_client] TTS error: {exc}")
        return None

"""
tests/test_decision_engine.py
pytest tests/test_decision_engine.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from croppulse.decision_engine import make_verdict


def _verdict(pct_change, trend, rain_signal, rain_mm=0.0):
    price_trend = {
        "crop": "Onion", "mandi": "Nashik", "date": "2024-10-15",
        "modal_price": 820.0, "avg_7d": 940.0, "avg_30d": 1050.0,
        "pct_change_7d": pct_change, "trend": trend,
    }
    weather = {
        "mandi": "Nashik", "rain_3d_mm": rain_mm,
        "signal": rain_signal, "description": "test"
    }
    return make_verdict(price_trend, weather)


def test_sell_now_falling_wet():
    v = _verdict(-13.0, "FALLING", "WET", rain_mm=7.0)
    assert v["decision"] == "SELL NOW"
    assert v["confidence"] == "HIGH"


def test_sell_soon_falling_dry():
    v = _verdict(-10.0, "FALLING", "DRY", rain_mm=0.0)
    assert v["decision"] == "SELL SOON"
    assert v["confidence"] == "MEDIUM"


def test_hold_rising_dry():
    v = _verdict(8.0, "RISING", "DRY", rain_mm=0.0)
    assert v["decision"] == "HOLD"
    assert v["confidence"] == "HIGH"


def test_hold_short_rising_wet():
    v = _verdict(6.0, "RISING", "WET", rain_mm=8.0)
    assert v["decision"] == "HOLD SHORT"
    assert v["confidence"] == "MEDIUM"


def test_monitor_stable():
    v = _verdict(2.0, "STABLE", "DRY", rain_mm=0.0)
    assert v["decision"] == "MONITOR"
    assert v["confidence"] == "LOW"


def test_verdict_has_required_keys():
    v = _verdict(-5.0, "FALLING", "WET", rain_mm=6.0)
    for key in ["decision", "confidence", "reason_en", "price_trend", "weather"]:
        assert key in v, f"Missing key: {key}"


def test_reason_not_empty():
    v = _verdict(-5.0, "FALLING", "WET", rain_mm=6.0)
    assert len(v["reason_en"]) > 20

"""
weather_utils.py — fetches 3-day rainfall forecast from OpenWeatherMap.
Returns a structured signal dict consumed by decision_engine.py.
"""
from __future__ import annotations

import requests
from croppulse.config import OPENWEATHER_API_KEY, MANDI_COORDS, RAIN_THRESHOLD_MM


def get_weather_signal(mandi: str) -> dict:
    """
    Fetch 3-day accumulated rainfall for a mandi district.

    Returns
    -------
    {
        "mandi": str,
        "rain_3d_mm": float,
        "signal": "WET" | "DRY",
        "description": str,
    }
    """
    if mandi not in MANDI_COORDS:
        return _no_data_signal(mandi)

    coords = MANDI_COORDS[mandi]
    url = (
        "https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={coords['lat']}&lon={coords['lon']}"
        f"&appid={OPENWEATHER_API_KEY}&units=metric&cnt=24"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"[weather_utils] API error: {exc} — falling back to DRY signal")
        return _no_data_signal(mandi)

    # Sum rainfall across next 3 days (8 x 3hr slots per day = 24 slots)
    rain_mm = sum(
        item.get("rain", {}).get("3h", 0.0)
        for item in data.get("list", [])
    )

    signal = "WET" if rain_mm >= RAIN_THRESHOLD_MM else "DRY"

    return {
        "mandi": mandi,
        "rain_3d_mm": round(rain_mm, 1),
        "signal": signal,
        "description": (
            f"{round(rain_mm, 1)} mm rain forecast over next 3 days near {mandi}"
        ),
    }


def _no_data_signal(mandi: str) -> dict:
    return {
        "mandi": mandi,
        "rain_3d_mm": 0.0,
        "signal": "DRY",
        "description": f"No weather data available for {mandi}.",
    }

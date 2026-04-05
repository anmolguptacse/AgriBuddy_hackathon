"""
llm_client.py — calls Databricks AI Gateway (Llama 4 Maverick).
Mirrors the pattern in nyaya-dhwani-hackathon.
"""
from __future__ import annotations

import os
import requests

from croppulse.config import AI_GATEWAY_ENDPOINT


def _get_client():
    """Return an OpenAI-compatible client pointed at Databricks AI Gateway."""
    try:
        from openai import OpenAI
        import mlflow.deployments
        client = OpenAI(
            api_key  = os.getenv("DATABRICKS_TOKEN", ""),
            base_url = f"{os.getenv('DATABRICKS_HOST', '')}/serving-endpoints",
        )
        return client
    except Exception:
        return None


def build_prompt(verdict: dict, advisory_context: str) -> str:
    """
    Construct the system + user prompt combining:
    - price trend data
    - weather signal
    - ICAR RAG context
    - decision verdict
    """
    pt = verdict["price_trend"]
    wx = verdict["weather"]

    system_prompt = (
        "You are CropPulse, an agricultural advisor for Indian smallholder farmers. "
        "Give clear, concise advice based on the price data and government advisory provided. "
        "Cite the specific advisory source at the end. "
        "Keep the response under 100 words. Do not use markdown formatting."
    )

    user_prompt = f"""
Crop: {pt['crop']}
Mandi (home): {pt['mandi']}
Today's price: ₹{pt['modal_price']:.0f}/quintal
7-day price change: {pt['pct_change_7d']:+.1f}%
Price trend: {pt['trend']}
Weather forecast: {wx['description']}
Verdict: {verdict['decision']} (confidence: {verdict['confidence']})
Reason: {verdict['reason_en']}

Government Advisory Context:
{advisory_context}

Write a farmer-friendly recommendation in plain English (2-3 sentences). End with: "Source: [advisory name]"
""".strip()

    return system_prompt, user_prompt


def get_recommendation(verdict: dict, advisory_context: str) -> str:
    """
    Call Databricks AI Gateway and return the English recommendation string.
    Falls back to a template-based answer if the API is unavailable.
    """
    system_prompt, user_prompt = build_prompt(verdict, advisory_context)

    client = _get_client()
    if client is not None:
        try:
            response = client.chat.completions.create(
                model    = AI_GATEWAY_ENDPOINT,
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                max_tokens  = 200,
                temperature = 0.2,
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            print(f"[llm_client] API error: {exc} — using fallback")

    # Template fallback
    pt = verdict["price_trend"]
    return (
        f"{verdict['decision']}: {verdict['reason_en']} "
        f"Today's price at {pt['mandi']} is ₹{pt['modal_price']:.0f}/quintal "
        f"({pt['pct_change_7d']:+.1f}% vs last week)."
    )

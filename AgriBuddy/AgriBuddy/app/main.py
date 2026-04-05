# """
# app/main.py — CropPulse FastAPI application with Genie Chat integration.
# Entry point for Databricks Apps (referenced in app.yaml).

# Switched from Gradio to FastAPI + plain HTML to avoid Gradio/huggingface_hub
# version conflicts in Databricks Apps environment.

# FastAPI + uvicorn are already installed (they are Gradio dependencies),
# so no new packages needed in requirements.txt.
# """
# from __future__ import annotations
# import os, sys, io, base64

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# # ── Load secrets from Databricks scope FIRST ─────────────────────────────────
# def _load_secrets_from_scope(scope: str = "croppulse") -> None:
#     secrets_map = {
#         "SARVAM_API_KEY":      "sarvam_api_key",
#         "OPENWEATHER_API_KEY": "openweather_api_key",
#         "DATA_GOV_API_KEY":    "data_gov_api_key",
#     }
#     try:
#         from databricks.sdk import WorkspaceClient
#         w = WorkspaceClient()
#         for env_var, key in secrets_map.items():
#             if not os.environ.get(env_var):
#                 try:
#                     val = w.secrets.get_secret(scope, key).value
#                     if val:
#                         os.environ[env_var] = val
#                 except Exception:
#                     pass
#         print(f"[app] Secret scope '{scope}' loaded")
#     except Exception as e:
#         print(f"[app] Secret scope unavailable ({e}) — using config.py values")


# _load_secrets_from_scope()

# # ── Imports after secrets ─────────────────────────────────────────────────────
# from fastapi import FastAPI
# from fastapi.responses import HTMLResponse, JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel

# from croppulse.config          import CROPS, MANDIS, SUPPORTED_LANGUAGES, GENIE_SPACE_ID
# from croppulse.decision_engine import get_price_trend, make_verdict
# from croppulse.weather_utils   import get_weather_signal
# from croppulse.rag_retrieval   import retrieve, format_context
# from croppulse.llm_client      import get_recommendation
# from croppulse.sarvam_client   import translate
# from croppulse.chart_utils     import build_price_chart, get_all_mandi_prices
# from croppulse.genie_client    import GenieClient

# print("[app] CropPulse FastAPI starting…")

# app = FastAPI(title="CropPulse")
# app.add_middleware(
#     CORSMiddleware, allow_origins=["*"],
#     allow_methods=["*"], allow_headers=["*"]
# )

# # ── Initialize Genie client ───────────────────────────────────────────────────
# genie = GenieClient(space_id=GENIE_SPACE_ID)
# print(f"[app] Genie client initialized with space: {GENIE_SPACE_ID}")

# # ── Pydantic models ───────────────────────────────────────────────────────────
# class ChatRequest(BaseModel):
#     question: str

# # ── Build HTML once at startup ────────────────────────────────────────────────
# CROP_OPTIONS  = "".join(f"<option>{c}</option>" for c in CROPS)
# MANDI_OPTIONS = "".join(f"<option>{m}</option>" for m in MANDIS)
# LANG_OPTIONS  = "".join(f"<option>{l}</option>" for l in SUPPORTED_LANGUAGES)

# HTML = f"""<!DOCTYPE html>
# <html lang="en">
# <head>
#   <meta charset="UTF-8">
#   <meta name="viewport" content="width=device-width,initial-scale=1">
#   <title>CropPulse — AI Advisor for Indian Farmers</title>
#   <style>
#     *{{box-sizing:border-box;margin:0;padding:0}}
#     body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7f7f5;color:#1a1a1a}}
#     .hdr{{background:#1D9E75;color:#fff;padding:20px 32px}}
#     .hdr h1{{font-size:22px;font-weight:600}}
#     .hdr p{{font-size:13px;opacity:.85;margin-top:4px}}
#     .wrap{{max-width:900px;margin:28px auto;padding:0 18px}}
#     .card{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;padding:22px;margin-bottom:18px}}
#     .inputs{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}}
#     label{{font-size:11px;font-weight:600;color:#666;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}}
#     select{{width:100%;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px;background:#fff}}
#     .btn{{width:100%;padding:13px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:500;cursor:pointer}}
#     .btn:hover{{background:#178a65}}
#     .btn:disabled{{background:#aaa;cursor:not-allowed}}
#     .badge{{display:inline-block;padding:7px 20px;border-radius:8px;font-size:20px;font-weight:700;color:#fff;margin-bottom:12px}}
#     .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
#     .row{{display:flex;justify-content:space-between;padding:7px 0;border-bottom:.5px solid #f0f0f0;font-size:13px}}
#     .lbl{{color:#888}}
#     .adv{{background:#E1F5EE;border-left:3px solid #1D9E75;border-radius:0 8px 8px 0;padding:12px 14px;margin-top:14px}}
#     .adv-lbl{{font-size:10px;font-weight:700;color:#085041;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}}
#     .adv-txt{{font-size:13px;color:#0F6E56;line-height:1.6}}
#     .adv-src{{font-size:11px;color:#1D9E75;margin-top:5px}}
#     .rec{{background:#f9f9f7;border-radius:8px;padding:13px;font-size:14px;line-height:1.7;margin-top:13px}}
#     img.chart{{width:100%;border-radius:8px}}
#     .loading{{text-align:center;padding:36px;color:#888;font-size:14px}}
#     .err{{background:#fff0f0;border:.5px solid #fcc;border-radius:8px;padding:13px;color:#c00;font-size:13px}}
#     .hidden{{display:none}}
#     .conf{{font-size:13px;color:#888;margin-bottom:14px}}
#     .foot{{text-align:center;font-size:11px;color:#bbb;padding:20px 0}}
    
#     /* Chat panel styles */
#     .chat-panel{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;margin-top:18px;overflow:hidden}}
#     .chat-header{{background:#1D9E75;color:#fff;padding:14px 18px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}}
#     .chat-header h3{{font-size:16px;font-weight:600}}
#     .chat-toggle{{font-size:18px}}
#     .chat-body{{padding:18px;max-height:500px;overflow-y:auto;display:none}}
#     .chat-body.open{{display:block}}
#     .chat-messages{{margin-bottom:14px;min-height:200px}}
#     .chat-msg{{margin-bottom:12px;padding:10px 12px;border-radius:8px;font-size:13px;line-height:1.5}}
#     .chat-msg.user{{background:#E1F5EE;text-align:right}}
#     .chat-msg.genie{{background:#f5f5f5}}
#     .chat-msg.error{{background:#fff0f0;color:#c00}}
#     .chat-input-area{{display:flex;gap:8px}}
#     .chat-input{{flex:1;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px}}
#     .chat-btn{{padding:10px 20px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer}}
#     .chat-btn:hover{{background:#178a65}}
#     .chat-btn:disabled{{background:#aaa;cursor:not-allowed}}
#     .chat-sql{{background:#f8f8f8;padding:8px;border-radius:4px;font-family:monospace;font-size:11px;margin-top:6px;overflow-x:auto}}
    
#     @media(max-width:600px){{.inputs,.grid2{{grid-template-columns:1fr}}}}
#   </style>
# </head>
# <body>
#   <div class="hdr">
#     <h1>CropPulse — AI Advisor for Indian Farmers</h1>
#     <p>Data-driven SELL or HOLD recommendations · Mandi prices · Weather · ICAR advisories</p>
#   </div>

#   <div class="wrap">
#     <div class="card">
#       <div class="inputs">
#         <div><label>Crop</label>
#           <select id="crop">{CROP_OPTIONS}</select></div>
#         <div><label>Home Mandi</label>
#           <select id="mandi">{MANDI_OPTIONS}</select></div>
#         <div><label>Language</label>
#           <select id="lang">{LANG_OPTIONS}</select></div>
#       </div>
#       <button class="btn" id="btn" onclick="go()">Get Advisory</button>
#     </div>

#     <div id="load" class="card loading hidden">Fetching live prices, weather and ICAR advisory…</div>
#     <div id="err"  class="err hidden"></div>

#     <div id="res" class="hidden">
#       <div class="grid2">
#         <div class="card">
#           <div id="badge" class="badge"></div>
#           <div id="conf"  class="conf"></div>
#           <div id="rows"></div>
#           <div class="adv">
#             <div class="adv-lbl">Government Advisory (ICAR)</div>
#             <div id="atxt" class="adv-txt"></div>
#             <div id="asrc" class="adv-src"></div>
#           </div>
#           <div id="rec" class="rec"></div>
#         </div>
#         <div class="card">
#           <div style="font-size:12px;color:#888;margin-bottom:8px">
#             Past 10 days (solid) · 10-day forecast (dashed) · Mandi comparison
#           </div>
#           <img id="chart" class="chart" src="" alt="Price chart">
#         </div>
#       </div>
#     </div>

#     <!-- Chat Panel -->
#     <div class="chat-panel">
#       <div class="chat-header" onclick="toggleChat()">
#         <h3>💬 Ask Questions About Mandi Prices</h3>
#         <span class="chat-toggle" id="chatToggle">▼</span>
#       </div>
#       <div class="chat-body" id="chatBody">
#         <div class="chat-messages" id="chatMessages">
#           <div class="chat-msg genie">
#             Hi! Ask me anything about mandi prices, crops, and market trends. For example: "What are the average onion prices in Pune?" or "Show me price trends for tomatoes"
#           </div>
#         </div>
#         <div class="chat-input-area">
#           <input type="text" id="chatInput" class="chat-input" placeholder="Ask about mandi prices..." onkeypress="if(event.key==='Enter')sendChat()">
#           <button class="chat-btn" id="chatBtn" onclick="sendChat()">Send</button>
#         </div>
#       </div>
#     </div>
#   </div>

#   <div class="foot">
#     APMC Agmarknet · OpenWeatherMap · ICAR India · Sarvam Mayura · Databricks Llama 4 Maverick · Genie AI
#   </div>

#   <script>
#     const C={{"SELL NOW":"#E24B4A","SELL SOON":"#EF9F27","HOLD":"#1D9E75","HOLD SHORT":"#378ADD","MONITOR":"#888780"}};
    
#     async function go(){{
#       const crop=document.getElementById('crop').value,
#             mandi=document.getElementById('mandi').value,
#             lang=document.getElementById('lang').value;
#       document.getElementById('btn').disabled=true;
#       document.getElementById('load').classList.remove('hidden');
#       document.getElementById('res').classList.add('hidden');
#       document.getElementById('err').classList.add('hidden');
#       try{{
#         const r=await fetch(`/advisory?crop=${{encodeURIComponent(crop)}}&mandi=${{encodeURIComponent(mandi)}}&lang=${{encodeURIComponent(lang)}}`);
#         const d=await r.json();
#         if(d.error)throw new Error(d.error);
#         const col=C[d.decision]||'#888';
#         document.getElementById('badge').textContent=d.decision;
#         document.getElementById('badge').style.background=col;
#         document.getElementById('conf').textContent='Confidence: '+d.confidence;
#         document.getElementById('atxt').textContent=d.advisory_text;
#         document.getElementById('asrc').textContent=d.advisory_source;
#         document.getElementById('rec').textContent=d.recommendation;
#         document.getElementById('chart').src='data:image/png;base64,'+d.chart_b64;
#         document.getElementById('rows').innerHTML=`
#           <div class="row"><span class="lbl">Today's price</span><b>₹${{d.modal_price}}/quintal</b></div>
#           <div class="row"><span class="lbl">7-day change</span><b style="color:${{col}}">${{d.pct_change}}%</b></div>
#           <div class="row"><span class="lbl">Price trend</span><b>${{d.trend}}</b></div>
#           <div class="row"><span class="lbl">Weather (3-day)</span><b>${{d.weather}}</b></div>
#           ${{Object.entries(d.all_prices).map(([m,p])=>`<div class="row"><span class="lbl">${{m}} today</span><b>₹${{p}}/q</b></div>`).join('')}}
#         `;
#         document.getElementById('res').classList.remove('hidden');
#       }}catch(e){{
#         document.getElementById('err').textContent='Error: '+e.message;
#         document.getElementById('err').classList.remove('hidden');
#       }}finally{{
#         document.getElementById('load').classList.add('hidden');
#         document.getElementById('btn').disabled=false;
#       }}
#     }}
    
#     function toggleChat(){{
#       const body=document.getElementById('chatBody');
#       const toggle=document.getElementById('chatToggle');
#       if(body.classList.contains('open')){{
#         body.classList.remove('open');
#         toggle.textContent='▼';
#       }}else{{
#         body.classList.add('open');
#         toggle.textContent='▲';
#       }}
#     }}
    
#     async function sendChat(){{
#       const input=document.getElementById('chatInput');
#       const btn=document.getElementById('chatBtn');
#       const messages=document.getElementById('chatMessages');
#       const question=input.value.trim();
      
#       if(!question)return;
      
#       // Add user message
#       const userMsg=document.createElement('div');
#       userMsg.className='chat-msg user';
#       userMsg.textContent=question;
#       messages.appendChild(userMsg);
      
#       input.value='';
#       btn.disabled=true;
      
#       // Add loading indicator
#       const loadingMsg=document.createElement('div');
#       loadingMsg.className='chat-msg genie';
#       loadingMsg.textContent='🤔 Thinking...';
#       loadingMsg.id='loadingMsg';
#       messages.appendChild(loadingMsg);
#       messages.scrollTop=messages.scrollHeight;
      
#       try{{
#         const r=await fetch('/chat/ask',{{
#           method:'POST',
#           headers:{{'Content-Type':'application/json'}},
#           body:JSON.stringify({{question}})
#         }});
#         const d=await r.json();
        
#         // Remove loading
#         document.getElementById('loadingMsg').remove();
        
#         if(d.error){{
#           const errMsg=document.createElement('div');
#           errMsg.className='chat-msg error';
#           errMsg.textContent='Error: '+d.error;
#           messages.appendChild(errMsg);
#         }}else{{
#           // Add answer
#           const answerMsg=document.createElement('div');
#           answerMsg.className='chat-msg genie';
#           let content=d.answer||'Query executed successfully.';
          
#           // Add SQL if available
#           if(d.sql){{
#             content+=`<div class="chat-sql">${{d.sql}}</div>`;
#           }}
          
#           // Add data preview if available
#           if(d.data && d.data.length>0){{
#             content+=`<div style="margin-top:8px;font-size:11px;color:#666">Returned ${{d.data.length}} rows</div>`;
#           }}
          
#           answerMsg.innerHTML=content;
#           messages.appendChild(answerMsg);
#         }}
#       }}catch(e){{
#         document.getElementById('loadingMsg').remove();
#         const errMsg=document.createElement('div');
#         errMsg.className='chat-msg error';
#         errMsg.textContent='Error: '+e.message;
#         messages.appendChild(errMsg);
#       }}finally{{
#         btn.disabled=false;
#         messages.scrollTop=messages.scrollHeight;
#       }}
#     }}
#   </script>
# </body>
# </html>"""


# # ── API routes ────────────────────────────────────────────────────────────────
# @app.get("/", response_class=HTMLResponse)
# def index():
#     return HTML


# # @app.get("/advisory")
# # def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "Hindi"):
# #     try:
# #         trend   = get_price_trend(crop, mandi)
# #         weather = get_weather_signal(mandi)
# #         verdict = make_verdict(trend, weather)
# #         chunks  = retrieve(f"{crop} storage harvest price", crop=crop)
# #         context = format_context(chunks)
# #         rec_en  = get_recommendation(verdict, context)

# #         recommendation  = translate(rec_en, lang)
# #         advisory_text   = translate(
# #             chunks[0]["text"] if chunks else "No advisory found.", lang
# #         )
# #         advisory_source = (
# #             f"Source: {chunks[0]['source_doc']} | Page {chunks[0]['page']}"
# #             if chunks else ""
# #         )

# #         chart_img = build_price_chart(crop, mandi, MANDIS)
# #         buf = io.BytesIO()
# #         chart_img.save(buf, format="PNG")
# #         chart_b64 = base64.b64encode(buf.getvalue()).decode()

# #         all_prices = get_all_mandi_prices(crop, MANDIS)
# #         pt = verdict["price_trend"]
# #         wx = verdict["weather"]

# #         return JSONResponse({
# #             "decision":        verdict["decision"],
# #             "confidence":      verdict["confidence"],
# #             "modal_price":     int(pt["modal_price"]),
# #             "pct_change":      f"{pt['pct_change_7d']:+.1f}",
# #             "trend":           pt["trend"],
# #             "weather":         f"{wx['signal']} — {wx['rain_3d_mm']} mm",
# #             "recommendation":  recommendation,
# #             "advisory_text":   advisory_text,
# #             "advisory_source": advisory_source,
# #             "chart_b64":       chart_b64,
# #             "all_prices":      {k: int(v) for k, v in all_prices.items()},
# #         })

# #     except Exception as exc:
# #         import traceback
# #         traceback.print_exc()
# #         return JSONResponse({"error": str(exc)}, status_code=500)

# # @app.get("/advisory")
# # def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "Hindi"):
# #     try:
# #         trend   = get_price_trend(crop, mandi)
# #         weather = get_weather_signal(mandi)
# #         verdict = make_verdict(trend, weather)
# #         chunks  = retrieve(f"{crop} storage harvest price", crop=crop)
# #         context = format_context(chunks)
# #         rec_en  = get_recommendation(verdict, context)

# #         recommendation  = translate(rec_en, lang)
# #         advisory_text   = translate(
# #             chunks[0]["text"] if chunks else "No advisory found.", lang
# #         )
# #         advisory_source = (
# #             f"Source: {chunks[0]['source_doc']} | Page {chunks[0]['page']}"
# #             if chunks else ""
# #         )

# #         chart_img = build_price_chart(crop, mandi, MANDIS)
# #         buf = io.BytesIO()
# #         chart_img.save(buf, format="PNG")
# #         chart_b64 = base64.b64encode(buf.getvalue()).decode()

# #         all_prices = get_all_mandi_prices(crop, MANDIS)
# #         pt = verdict["price_trend"]
# #         wx = verdict["weather"]

# #         return JSONResponse({
# #             "decision":        verdict["decision"],
# #             "confidence":      verdict["confidence"],
# #             "modal_price":     int(pt["modal_price"]),
# #             "pct_change":      f"{pt['pct_change_7d']:+.1f}",
# #             "trend":           pt["trend"],
# #             "weather":         f"{wx['signal']} — {wx['rain_3d_mm']} mm",
# #             "recommendation":  recommendation,
# #             "advisory_text":   advisory_text,
# #             "advisory_source": advisory_source,
# #             "chart_b64":       chart_b64,
# #             "all_prices":      {k: int(v) for k, v in all_prices.items()},
# #         })

# #     except Exception as exc:
# #         import traceback
# #         traceback.print_exc()
# #         # Always return JSON — never let the proxy see a crash
# #         return JSONResponse(
# #             {"error": str(exc)},
# #             status_code=200   # return 200 so browser gets the JSON, not proxy error page
# #         )
# @app.get("/advisory")
# def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "Hindi"):
#     try:

#         # ── 1. Price trend ────────────────────────────────────────────────────
#         try:
#             trend = get_price_trend(crop, mandi)
#         except Exception as e:
#             print(f"[advisory] price_trend failed: {e}")
#             trend = {
#                 "crop": crop, "mandi": mandi, "date": "2026-04-05",
#                 "modal_price": 820.0, "avg_7d": 940.0, "avg_30d": 1050.0,
#                 "pct_change_7d": -12.8, "trend": "FALLING",
#             }

#         # ── 2. Weather ────────────────────────────────────────────────────────
#         try:
#             weather = get_weather_signal(mandi)
#         except Exception as e:
#             print(f"[advisory] weather failed: {e}")
#             weather = {"mandi": mandi, "rain_3d_mm": 0.0,
#                        "signal": "DRY", "description": "Weather data unavailable."}

#         # ── 3. Decision verdict ───────────────────────────────────────────────
#         try:
#             verdict = make_verdict(trend, weather)
#         except Exception as e:
#             print(f"[advisory] verdict failed: {e}")
#             verdict = {
#                 "decision": "MONITOR", "confidence": "LOW",
#                 "reason_en": "Unable to compute verdict — check data pipeline.",
#                 "price_trend": trend, "weather": weather,
#             }

#         # ── 4. RAG advisory (always works — uses mock chunks as fallback) ─────
#         try:
#             chunks  = retrieve(f"{crop} storage harvest price", crop=crop)
#             context = format_context(chunks)
#         except Exception as e:
#             print(f"[advisory] RAG failed: {e}")
#             chunks = []
#             context = ""

#         # If no chunks from RAG, use hardcoded advisory text
#         if not chunks:
#             if crop == "Onion":
#                 advisory_text_en  = (
#                     "Kharif onion should be sold within 10 days of harvest when "
#                     "ambient humidity exceeds 75%. Prolonged storage under wet "
#                     "conditions increases rotting losses by 30-40%. Monitor APMC "
#                     "prices at Nashik and Pune daily before committing to a sale."
#                 )
#                 advisory_source = "Source: ICAR Onion Post-Harvest Advisory | Page 4"
#             else:
#                 advisory_text_en  = (
#                     "Tomato prices are highly sensitive to rainfall. A 10mm rainfall "
#                     "event typically causes a 15-20% price drop within 5 days. "
#                     "Sell within 3-4 days of harvest for table grade produce."
#                 )
#                 advisory_source = "Source: ICAR Tomato Pest Management Guide | Page 12"
#         else:
#             advisory_text_en = chunks[0]["text"]
#             advisory_source  = f"Source: {chunks[0]['source_doc']} | Page {chunks[0]['page']}"

#         # ── 5. LLM recommendation ─────────────────────────────────────────────
#         try:
#             rec_en = get_recommendation(verdict, context)
#         except Exception as e:
#             print(f"[advisory] LLM failed: {e}")
#             pt = verdict["price_trend"]
#             wx = verdict["weather"]
#             rec_en = (
#                 f"{verdict['decision']}: Price has changed {pt['pct_change_7d']:+.1f}% "
#                 f"over 7 days at {mandi} mandi. Current price ₹{int(pt['modal_price'])}/quintal. "
#                 f"Weather: {wx['signal']} ({wx['rain_3d_mm']}mm forecast). "
#                 f"{verdict['reason_en']}"
#             )

#         # ── 6. Translation ────────────────────────────────────────────────────
#         try:
#             recommendation = translate(rec_en, lang)
#             advisory_text  = translate(advisory_text_en, lang)
#         except Exception as e:
#             print(f"[advisory] translation failed: {e}")
#             recommendation = rec_en
#             advisory_text  = advisory_text_en

#         # ── 7. Price chart ────────────────────────────────────────────────────
#         try:
#             chart_img = build_price_chart(crop, mandi, MANDIS)
#             buf = io.BytesIO()
#             chart_img.save(buf, format="PNG")
#             chart_b64 = base64.b64encode(buf.getvalue()).decode()
#         except Exception as e:
#             print(f"[advisory] chart failed: {e}")
#             # Return a 1x1 transparent PNG as fallback
#             chart_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

#         # ── 8. All mandi prices ───────────────────────────────────────────────
#         try:
#             all_prices = get_all_mandi_prices(crop, MANDIS)
#         except Exception as e:
#             print(f"[advisory] all_prices failed: {e}")
#             all_prices = {"Nashik": 820, "Pune": 1100}

#         pt = verdict["price_trend"]
#         wx = verdict["weather"]

#         return JSONResponse({
#             "decision":        verdict["decision"],
#             "confidence":      verdict["confidence"],
#             "modal_price":     int(pt["modal_price"]),
#             "pct_change":      f"{pt['pct_change_7d']:+.1f}",
#             "trend":           pt["trend"],
#             "weather":         f"{wx['signal']} — {wx['rain_3d_mm']} mm",
#             "recommendation":  recommendation,
#             "advisory_text":   advisory_text,
#             "advisory_source": advisory_source,
#             "chart_b64":       chart_b64,
#             "all_prices":      {k: int(v) for k, v in all_prices.items()},
#         })

#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": str(exc)}, status_code=200)

# # ── Chat endpoint ─────────────────────────────────────────────────────────────
# @app.post("/chat/ask")
# async def chat_ask(request: ChatRequest):
#     """Ask Genie a question and wait for the result."""
#     try:
#         result = genie.ask_question(request.question, timeout=60)
        
#         response_data = {
#             "conversation_id": result.get("conversation_id", ""),
#             "message_id": result.get("message_id", ""),
#             "status": result["status"],
#             "answer": result.get("content", ""),
#         }
        
#         # Extract SQL from attachments if available
#         if "attachments" in result and result["attachments"]:
#             for att in result["attachments"]:
#                 if "sql" in att:
#                     response_data["sql"] = att["sql"]
#                     break
        
#         # Extract data if available
#         if "query_result" in result and result["query_result"]:
#             qr = result["query_result"]
#             if "data" in qr and qr["data"]:
#                 response_data["data"] = qr["data"][:10]  # Limit to 10 rows for UI
        
#         # Add error if present
#         if "error" in result:
#             response_data["error"] = result["error"]
        
#         return JSONResponse(response_data)
        
#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": str(exc)}, status_code=500)


# @app.get("/health")
# def health():
#     return {"status": "ok", "app": "CropPulse"}

# # Add this — Databricks Apps health check endpoint
# @app.get("/metrics")
# def metrics():
#     return {"status": "ok"}

# # ── Entry point ───────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     import uvicorn
#     port = int(os.getenv("PORT", 8080))
#     print(f"[app] Starting on http://0.0.0.0:{port}")
#     uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")




# ##################################################
# ### new main dummy 
# from __future__ import annotations
# import os, sys, io, base64

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# from fastapi import FastAPI
# from fastapi.responses import HTMLResponse, JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
# import matplotlib
# matplotlib.use("Agg")
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import pandas as pd
# import numpy as np

# app = FastAPI()
# app.add_middleware(CORSMiddleware, allow_origins=["*"],
#                    allow_methods=["*"], allow_headers=["*"])

# CROPS  = ["Onion", "Tomato"]
# MANDIS = ["Nashik", "Pune"]
# LANGS  = ["English", "Hindi", "Marathi", "Telugu", "Tamil", "Kannada", "Gujarati"]

# # ── Realistic mock data per crop/mandi ────────────────────────────────────────
# PRICE_DATA = {
#     ("Onion",  "Nashik"): {"modal_price": 820,  "pct_change": -12.8, "trend": "FALLING"},
#     ("Onion",  "Pune"):   {"modal_price": 1100, "pct_change":  +4.8, "trend": "RISING"},
#     ("Tomato", "Nashik"): {"modal_price": 750,  "pct_change":  +7.1, "trend": "RISING"},
#     ("Tomato", "Pune"):   {"modal_price": 900,  "pct_change":  -6.3, "trend": "FALLING"},
# }

# ADVISORIES = {
#     "Onion": {
#         "text": "Kharif onion should be sold within 10 days of harvest when ambient humidity exceeds 75%. Prolonged storage under wet conditions increases rotting losses by 30-40%. Monitor APMC prices at Nashik and Pune daily before committing to a sale.",
#         "source": "Source: ICAR Onion Post-Harvest Advisory | Page 4",
#     },
#     "Tomato": {
#         "text": "Tomato prices are highly sensitive to rainfall. A 10mm rainfall event typically causes a 15-20% price drop within 5 days. Sell within 3-4 days of harvest for table grade produce.",
#         "source": "Source: ICAR Tomato Pest Management Guide | Page 12",
#     },
# }

# CROP_OPTIONS  = "".join(f"<option>{c}</option>" for c in CROPS)
# MANDI_OPTIONS = "".join(f"<option>{m}</option>" for m in MANDIS)
# LANG_OPTIONS  = "".join(f"<option>{l}</option>" for l in LANGS)

# HTML = f"""<!DOCTYPE html>
# <html lang="en">
# <head>
#   <meta charset="UTF-8">
#   <meta name="viewport" content="width=device-width,initial-scale=1">
#   <title>CropPulse — AI Advisor for Indian Farmers</title>
#   <style>
#     *{{box-sizing:border-box;margin:0;padding:0}}
#     body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7f7f5;color:#1a1a1a}}
#     .hdr{{background:#1D9E75;color:#fff;padding:20px 32px}}
#     .hdr h1{{font-size:22px;font-weight:600}}
#     .hdr p{{font-size:13px;opacity:.85;margin-top:4px}}
#     .wrap{{max-width:900px;margin:28px auto;padding:0 18px}}
#     .card{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;padding:22px;margin-bottom:18px}}
#     .inputs{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}}
#     label{{font-size:11px;font-weight:600;color:#666;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}}
#     select{{width:100%;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px;background:#fff}}
#     .btn{{width:100%;padding:13px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:500;cursor:pointer}}
#     .btn:hover{{background:#178a65}}
#     .btn:disabled{{background:#aaa;cursor:not-allowed}}
#     .badge{{display:inline-block;padding:7px 20px;border-radius:8px;font-size:20px;font-weight:700;color:#fff;margin-bottom:12px}}
#     .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
#     .row{{display:flex;justify-content:space-between;padding:7px 0;border-bottom:.5px solid #f0f0f0;font-size:13px}}
#     .lbl{{color:#888}}
#     .adv{{background:#E1F5EE;border-left:3px solid #1D9E75;border-radius:0 8px 8px 0;padding:12px 14px;margin-top:14px}}
#     .adv-lbl{{font-size:10px;font-weight:700;color:#085041;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}}
#     .adv-txt{{font-size:13px;color:#0F6E56;line-height:1.6}}
#     .adv-src{{font-size:11px;color:#1D9E75;margin-top:5px}}
#     .rec{{background:#f9f9f7;border-radius:8px;padding:13px;font-size:14px;line-height:1.7;margin-top:13px}}
#     img.chart{{width:100%;border-radius:8px}}
#     .loading{{text-align:center;padding:36px;color:#888;font-size:14px}}
#     .err{{background:#fff0f0;border:.5px solid #fcc;border-radius:8px;padding:13px;color:#c00;font-size:13px}}
#     .hidden{{display:none}}
#     .conf{{font-size:13px;color:#888;margin-bottom:14px}}
#     .foot{{text-align:center;font-size:11px;color:#bbb;padding:20px 0}}
#     @media(max-width:600px){{.inputs,.grid2{{grid-template-columns:1fr}}}}
#   </style>
# </head>
# <body>
#   <div class="hdr">
#     <h1>CropPulse — AI Advisor for Indian Farmers</h1>
#     <p>Data-driven SELL or HOLD recommendations · Mandi prices · Weather · ICAR advisories</p>
#   </div>
#   <div class="wrap">
#     <div class="card">
#       <div class="inputs">
#         <div><label>Crop</label><select id="crop">{CROP_OPTIONS}</select></div>
#         <div><label>Home Mandi</label><select id="mandi">{MANDI_OPTIONS}</select></div>
#         <div><label>Language</label><select id="lang">{LANG_OPTIONS}</select></div>
#       </div>
#       <button class="btn" id="btn" onclick="go()">Get Advisory</button>
#     </div>
#     <div id="load" class="card loading hidden">Fetching prices, weather and advisory…</div>
#     <div id="err" class="err hidden"></div>
#     <div id="res" class="hidden">
#       <div class="grid2">
#         <div class="card">
#           <div id="badge" class="badge"></div>
#           <div id="conf" class="conf"></div>
#           <div id="rows"></div>
#           <div class="adv">
#             <div class="adv-lbl">Government Advisory (ICAR)</div>
#             <div id="atxt" class="adv-txt"></div>
#             <div id="asrc" class="adv-src"></div>
#           </div>
#           <div id="rec" class="rec"></div>
#         </div>
#         <div class="card">
#           <div style="font-size:12px;color:#888;margin-bottom:8px">Past 10 days · 10-day forecast · Mandi comparison</div>
#           <img id="chart" class="chart" src="" alt="Price chart">
#         </div>
#       </div>
#     </div>
#   </div>
#   <div class="foot">APMC Agmarknet · OpenWeatherMap · ICAR India · Sarvam Mayura · Databricks Llama 4 Maverick</div>
#   <script>
#     const C={{"SELL NOW":"#E24B4A","SELL SOON":"#EF9F27","HOLD":"#1D9E75","HOLD SHORT":"#378ADD","MONITOR":"#888780"}};
#     async function go(){{
#       const crop=document.getElementById('crop').value,
#             mandi=document.getElementById('mandi').value,
#             lang=document.getElementById('lang').value;
#       document.getElementById('btn').disabled=true;
#       document.getElementById('load').classList.remove('hidden');
#       document.getElementById('res').classList.add('hidden');
#       document.getElementById('err').classList.add('hidden');
#       try{{
#         const r=await fetch(`/advisory?crop=${{encodeURIComponent(crop)}}&mandi=${{encodeURIComponent(mandi)}}&lang=${{encodeURIComponent(lang)}}`);
#         const d=await r.json();
#         if(d.error)throw new Error(d.error);
#         const col=C[d.decision]||'#888';
#         document.getElementById('badge').textContent=d.decision;
#         document.getElementById('badge').style.background=col;
#         document.getElementById('conf').textContent='Confidence: '+d.confidence;
#         document.getElementById('atxt').textContent=d.advisory_text;
#         document.getElementById('asrc').textContent=d.advisory_source;
#         document.getElementById('rec').textContent=d.recommendation;
#         document.getElementById('chart').src='data:image/png;base64,'+d.chart_b64;
#         document.getElementById('rows').innerHTML=`
#           <div class="row"><span class="lbl">Today's price</span><b>₹${{d.modal_price}}/quintal</b></div>
#           <div class="row"><span class="lbl">7-day change</span><b style="color:${{col}}">${{d.pct_change}}%</b></div>
#           <div class="row"><span class="lbl">Price trend</span><b>${{d.trend}}</b></div>
#           <div class="row"><span class="lbl">Weather (3-day)</span><b>${{d.weather}}</b></div>
#           ${{Object.entries(d.all_prices).map(([m,p])=>`<div class="row"><span class="lbl">${{m}} today</span><b>₹${{p}}/q</b></div>`).join('')}}
#         `;
#         document.getElementById('res').classList.remove('hidden');
#       }}catch(e){{
#         document.getElementById('err').textContent='Error: '+e.message;
#         document.getElementById('err').classList.remove('hidden');
#       }}finally{{
#         document.getElementById('load').classList.add('hidden');
#         document.getElementById('btn').disabled=false;
#       }}
#     }}
#   </script>
# </body>
# </html>"""


# def make_chart(crop: str, mandi: str) -> str:
#     """Generate price chart and return as base64 PNG."""
#     try:
#         today  = pd.Timestamp.today()
#         dates  = pd.date_range(end=today, periods=10, freq="D")
#         base   = PRICE_DATA.get((crop, mandi), {}).get("modal_price", 900)
#         trend  = PRICE_DATA.get((crop, mandi), {}).get("pct_change", 0)
#         prices = [max(200, base - (trend/10)*i + np.random.normal(0, 20))
#                   for i in range(10, 0, -1)]

#         f_start  = today + pd.Timedelta(days=1)
#         f_dates  = pd.date_range(start=f_start, periods=10, freq="D")
#         f_prices = [max(200, prices[-1] + trend*i/10) for i in range(1, 11)]
#         f_lower  = [p - 50 for p in f_prices]
#         f_upper  = [p + 50 for p in f_prices]

#         all_prices = {
#             "Nashik": PRICE_DATA.get((crop, "Nashik"), {}).get("modal_price", 800),
#             "Pune":   PRICE_DATA.get((crop, "Pune"),   {}).get("modal_price", 950),
#         }

#         fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6),
#                                         gridspec_kw={"height_ratios": [2, 1]},
#                                         facecolor="#ffffff")
#         fig.subplots_adjust(hspace=0.45)

#         ax1.plot(dates, prices, color="#1D9E75", linewidth=2.2,
#                  label="Actual price", marker="o", markersize=4)
#         ax1.plot([dates[-1], f_dates[0]], [prices[-1], f_prices[0]],
#                  color="#E24B4A", linewidth=1.5, linestyle="--")
#         ax1.plot(f_dates, f_prices, color="#E24B4A", linewidth=2,
#                  linestyle="--", label="10-day forecast")
#         ax1.fill_between(f_dates, f_lower, f_upper, alpha=0.12, color="#E24B4A")
#         ax1.axvline(dates[-1], color="#888780", linewidth=1, linestyle=":")
#         ax1.set_title(f"{crop} — {mandi} mandi price (₹/quintal)", fontsize=11, pad=8)
#         ax1.set_ylabel("₹ / quintal", fontsize=9)
#         ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
#         ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
#         ax1.tick_params(axis="both", labelsize=8)
#         ax1.legend(fontsize=8, loc="upper right")
#         ax1.grid(axis="y", alpha=0.25, linewidth=0.5)

#         sorted_m = sorted(all_prices.keys())
#         bar_prices = [all_prices[m] for m in sorted_m]
#         colors = ["#1D9E75" if m == mandi else "#B5D4F4" for m in sorted_m]
#         bars = ax2.bar(sorted_m, bar_prices, color=colors, width=0.45, edgecolor="none")
#         for bar, price in zip(bars, bar_prices):
#             ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height()+15,
#                      f"₹{int(price)}", ha="center", va="bottom",
#                      fontsize=9, fontweight="bold")
#         ax2.set_title("Today's price — all mandis", fontsize=10, pad=6)
#         ax2.set_ylabel("₹ / quintal", fontsize=9)
#         ax2.set_ylim(0, max(bar_prices)*1.25 if bar_prices else 1500)
#         ax2.grid(axis="y", alpha=0.2, linewidth=0.5)

#         buf = io.BytesIO()
#         fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
#         buf.seek(0)
#         plt.close(fig)
#         return base64.b64encode(buf.getvalue()).decode()
#     except Exception as e:
#         print(f"[chart] failed: {e}")
#         return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="


# @app.get("/", response_class=HTMLResponse)
# def index():
#     return HTML

# @app.get("/health")
# def health():
#     return {"status": "ok"}

# @app.get("/metrics")
# def metrics():
#     return {"status": "ok"}

# @app.get("/advisory")
# def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "English"):
#     try:
#         d       = PRICE_DATA.get((crop, mandi), {"modal_price": 800, "pct_change": 0.0, "trend": "STABLE"})
#         price   = d["modal_price"]
#         pct     = d["pct_change"]
#         trend   = d["trend"]

#         # Decision logic
#         if trend == "FALLING":
#             decision, confidence = "SELL SOON", "MEDIUM"
#             reason = f"Price is down {abs(pct):.1f}% over 7 days at {mandi}. Sell within 3-5 days."
#         elif trend == "RISING":
#             decision, confidence = "HOLD", "HIGH"
#             reason = f"Price is up {pct:.1f}% over 7 days at {mandi}. Hold for better returns."
#         else:
#             decision, confidence = "MONITOR", "LOW"
#             reason = f"Price is stable at {mandi}. Monitor daily before deciding."

#         adv = ADVISORIES.get(crop, ADVISORIES["Onion"])

#         # Try translation via Sarvam
#         recommendation = reason
#         advisory_text  = adv["text"]
#         if lang != "English":
#             try:
#                 from croppulse.sarvam_client import translate
#                 recommendation = translate(reason, lang)
#                 advisory_text  = translate(adv["text"], lang)
#             except Exception as e:
#                 print(f"[advisory] translation skipped: {e}")

#         chart_b64 = make_chart(crop, mandi)

#         return JSONResponse({
#             "decision":        decision,
#             "confidence":      confidence,
#             "modal_price":     int(price),
#             "pct_change":      f"{pct:+.1f}",
#             "trend":           trend,
#             "weather":         "DRY — 0.0 mm",
#             "recommendation":  recommendation,
#             "advisory_text":   advisory_text,
#             "advisory_source": adv["source"],
#             "chart_b64":       chart_b64,
#             "all_prices":      {
#                 "Nashik": int(PRICE_DATA.get((crop,"Nashik"),{}).get("modal_price",800)),
#                 "Pune":   int(PRICE_DATA.get((crop,"Pune"),{}).get("modal_price",950)),
#             },
#         })

#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": str(exc)}, status_code=200)


# if __name__ == "__main__":
#     import uvicorn
#     port = int(os.getenv("PORT", 8000))
#     print(f"[app] Starting on http://0.0.0.0:{port}")
#     uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")




# ##### main final version 
# from __future__ import annotations
# import os, sys, io, base64

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# from fastapi import FastAPI
# from fastapi.responses import HTMLResponse, JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel
# import matplotlib
# matplotlib.use("Agg")
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import pandas as pd
# import numpy as np

# # ── Genie Integration Imports ────────────────────────────────────────────────
# try:
#     from croppulse.config import GENIE_SPACE_ID
#     from croppulse.genie_client import GenieClient
# except ImportError:
#     print("[app] WARNING: croppulse modules not found. Using Mock GenieClient for testing.")
#     GENIE_SPACE_ID = "mock_space_id"
    
#     class GenieClient:
#         def __init__(self, space_id):
#             self.space_id = space_id
#         def ask_question(self, question, timeout):
#             return {
#                 "status": "success",
#                 "content": f"This is a mock answer for: '{question}'. Connect the real GenieClient to see actual responses.",
#                 "query_result": {"data": [{"mock_col": "mock_val"}]}
#             }

# app = FastAPI()
# app.add_middleware(CORSMiddleware, allow_origins=["*"],
#                    allow_methods=["*"], allow_headers=["*"])

# # ── Initialize Genie client ──────────────────────────────────────────────────
# genie = GenieClient(space_id=GENIE_SPACE_ID)
# print(f"[app] Genie client initialized with space: {GENIE_SPACE_ID}")

# class ChatRequest(BaseModel):
#     question: str

# CROPS  = ["Onion", "Tomato"]
# MANDIS = ["Nashik", "Pune"]
# LANGS  = ["English", "Hindi", "Marathi", "Telugu", "Tamil", "Kannada", "Gujarati"]

# # ── Realistic mock data per crop/mandi ────────────────────────────────────────
# PRICE_DATA = {
#     ("Onion",  "Nashik"): {"modal_price": 820,  "pct_change": -12.8, "trend": "FALLING"},
#     ("Onion",  "Pune"):   {"modal_price": 1100, "pct_change":  +4.8, "trend": "RISING"},
#     ("Tomato", "Nashik"): {"modal_price": 750,  "pct_change":  +7.1, "trend": "RISING"},
#     ("Tomato", "Pune"):   {"modal_price": 900,  "pct_change":  -6.3, "trend": "FALLING"},
# }

# ADVISORIES = {
#     "Onion": {
#         "text": "Kharif onion should be sold within 10 days of harvest when ambient humidity exceeds 75%. Prolonged storage under wet conditions increases rotting losses by 30-40%. Monitor APMC prices at Nashik and Pune daily before committing to a sale.",
#         "source": "Source: ICAR Onion Post-Harvest Advisory | Page 4",
#     },
#     "Tomato": {
#         "text": "Tomato prices are highly sensitive to rainfall. A 10mm rainfall event typically causes a 15-20% price drop within 5 days. Sell within 3-4 days of harvest for table grade produce.",
#         "source": "Source: ICAR Tomato Pest Management Guide | Page 12",
#     },
# }

# CROP_OPTIONS  = "".join(f"<option>{c}</option>" for c in CROPS)
# MANDI_OPTIONS = "".join(f"<option>{m}</option>" for m in MANDIS)
# LANG_OPTIONS  = "".join(f"<option>{l}</option>" for l in LANGS)

# HTML = f"""<!DOCTYPE html>
# <html lang="en">
# <head>
#   <meta charset="UTF-8">
#   <meta name="viewport" content="width=device-width,initial-scale=1">
#   <title>CropPulse — AI Advisor for Indian Farmers</title>
#   <style>
#     *{{box-sizing:border-box;margin:0;padding:0}}
#     body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7f7f5;color:#1a1a1a}}
#     .hdr{{background:#1D9E75;color:#fff;padding:20px 32px}}
#     .hdr h1{{font-size:22px;font-weight:600}}
#     .hdr p{{font-size:13px;opacity:.85;margin-top:4px}}
#     .wrap{{max-width:900px;margin:28px auto;padding:0 18px}}
#     .card{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;padding:22px;margin-bottom:18px}}
#     .inputs{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}}
#     label{{font-size:11px;font-weight:600;color:#666;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}}
#     select{{width:100%;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px;background:#fff}}
#     .btn{{width:100%;padding:13px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:500;cursor:pointer}}
#     .btn:hover{{background:#178a65}}
#     .btn:disabled{{background:#aaa;cursor:not-allowed}}
#     .badge{{display:inline-block;padding:7px 20px;border-radius:8px;font-size:20px;font-weight:700;color:#fff;margin-bottom:12px}}
#     .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
#     .row{{display:flex;justify-content:space-between;padding:7px 0;border-bottom:.5px solid #f0f0f0;font-size:13px}}
#     .lbl{{color:#888}}
#     .adv{{background:#E1F5EE;border-left:3px solid #1D9E75;border-radius:0 8px 8px 0;padding:12px 14px;margin-top:14px}}
#     .adv-lbl{{font-size:10px;font-weight:700;color:#085041;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}}
#     .adv-txt{{font-size:13px;color:#0F6E56;line-height:1.6}}
#     .adv-src{{font-size:11px;color:#1D9E75;margin-top:5px}}
#     .rec{{background:#f9f9f7;border-radius:8px;padding:13px;font-size:14px;line-height:1.7;margin-top:13px}}
#     img.chart{{width:100%;border-radius:8px}}
#     .loading{{text-align:center;padding:36px;color:#888;font-size:14px}}
#     .err{{background:#fff0f0;border:.5px solid #fcc;border-radius:8px;padding:13px;color:#c00;font-size:13px}}
#     .hidden{{display:none}}
#     .conf{{font-size:13px;color:#888;margin-bottom:14px}}
#     .foot{{text-align:center;font-size:11px;color:#bbb;padding:20px 0}}
    
#     /* Chat panel styles */
#     .chat-panel{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;margin-top:18px;overflow:hidden}}
#     .chat-header{{background:#1D9E75;color:#fff;padding:14px 18px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}}
#     .chat-header h3{{font-size:16px;font-weight:600}}
#     .chat-toggle{{font-size:18px}}
#     .chat-body{{padding:18px;max-height:500px;overflow-y:auto;display:none}}
#     .chat-body.open{{display:block}}
#     .chat-messages{{margin-bottom:14px;min-height:200px}}
#     .chat-msg{{margin-bottom:12px;padding:10px 12px;border-radius:8px;font-size:13px;line-height:1.5}}
#     .chat-msg.user{{background:#E1F5EE;text-align:right}}
#     .chat-msg.genie{{background:#f5f5f5}}
#     .chat-msg.error{{background:#fff0f0;color:#c00}}
#     .chat-input-area{{display:flex;gap:8px}}
#     .chat-input{{flex:1;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px}}
#     .chat-btn{{padding:10px 20px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer}}
#     .chat-btn:hover{{background:#178a65}}
#     .chat-btn:disabled{{background:#aaa;cursor:not-allowed}}
#     .chat-sql{{background:#f8f8f8;padding:8px;border-radius:4px;font-family:monospace;font-size:11px;margin-top:6px;overflow-x:auto}}
    
#     @media(max-width:600px){{.inputs,.grid2{{grid-template-columns:1fr}}}}
#   </style>
# </head>
# <body>
#   <div class="hdr">
#     <h1>CropPulse — AI Advisor for Indian Farmers</h1>
#     <p>Data-driven SELL or HOLD recommendations · Mandi prices · Weather · ICAR advisories</p>
#   </div>
#   <div class="wrap">
#     <div class="card">
#       <div class="inputs">
#         <div><label>Crop</label><select id="crop">{CROP_OPTIONS}</select></div>
#         <div><label>Home Mandi</label><select id="mandi">{MANDI_OPTIONS}</select></div>
#         <div><label>Language</label><select id="lang">{LANG_OPTIONS}</select></div>
#       </div>
#       <button class="btn" id="btn" onclick="go()">Get Advisory</button>
#     </div>
#     <div id="load" class="card loading hidden">Fetching prices, weather and advisory…</div>
#     <div id="err" class="err hidden"></div>
#     <div id="res" class="hidden">
#       <div class="grid2">
#         <div class="card">
#           <div id="badge" class="badge"></div>
#           <div id="conf" class="conf"></div>
#           <div id="rows"></div>
#           <div class="adv">
#             <div class="adv-lbl">Government Advisory (ICAR)</div>
#             <div id="atxt" class="adv-txt"></div>
#             <div id="asrc" class="adv-src"></div>
#           </div>
#           <div id="rec" class="rec"></div>
#         </div>
#         <div class="card">
#           <div style="font-size:12px;color:#888;margin-bottom:8px">Past 10 days · 10-day forecast · Mandi comparison</div>
#           <img id="chart" class="chart" src="" alt="Price chart">
#         </div>
#       </div>
#     </div>
    
#     <div class="chat-panel">
#       <div class="chat-header" onclick="toggleChat()">
#         <h3>💬 Ask Questions About Mandi Prices</h3>
#         <span class="chat-toggle" id="chatToggle">▼</span>
#       </div>
#       <div class="chat-body" id="chatBody">
#         <div class="chat-messages" id="chatMessages">
#           <div class="chat-msg genie">
#             Hi! Ask me anything about mandi prices, crops, and market trends. For example: "What are the average onion prices in Pune?" or "Show me price trends for tomatoes"
#           </div>
#         </div>
#         <div class="chat-input-area">
#           <input type="text" id="chatInput" class="chat-input" placeholder="Ask about mandi prices..." onkeypress="if(event.key==='Enter')sendChat()">
#           <button class="chat-btn" id="chatBtn" onclick="sendChat()">Send</button>
#         </div>
#       </div>
#     </div>
    
#   </div>
#   <div class="foot">APMC Agmarknet · OpenWeatherMap · ICAR India · Sarvam Mayura · Databricks Llama 4 Maverick · Genie AI</div>
#   <script>
#     const C={{"SELL NOW":"#E24B4A","SELL SOON":"#EF9F27","HOLD":"#1D9E75","HOLD SHORT":"#378ADD","MONITOR":"#888780"}};
#     async function go(){{
#       const crop=document.getElementById('crop').value,
#             mandi=document.getElementById('mandi').value,
#             lang=document.getElementById('lang').value;
#       document.getElementById('btn').disabled=true;
#       document.getElementById('load').classList.remove('hidden');
#       document.getElementById('res').classList.add('hidden');
#       document.getElementById('err').classList.add('hidden');
#       try{{
#         const r=await fetch(`/advisory?crop=${{encodeURIComponent(crop)}}&mandi=${{encodeURIComponent(mandi)}}&lang=${{encodeURIComponent(lang)}}`);
#         const d=await r.json();
#         if(d.error)throw new Error(d.error);
#         const col=C[d.decision]||'#888';
#         document.getElementById('badge').textContent=d.decision;
#         document.getElementById('badge').style.background=col;
#         document.getElementById('conf').textContent='Confidence: '+d.confidence;
#         document.getElementById('atxt').textContent=d.advisory_text;
#         document.getElementById('asrc').textContent=d.advisory_source;
#         document.getElementById('rec').textContent=d.recommendation;
#         document.getElementById('chart').src='data:image/png;base64,'+d.chart_b64;
#         document.getElementById('rows').innerHTML=`
#           <div class="row"><span class="lbl">Today's price</span><b>₹${{d.modal_price}}/quintal</b></div>
#           <div class="row"><span class="lbl">7-day change</span><b style="color:${{col}}">${{d.pct_change}}%</b></div>
#           <div class="row"><span class="lbl">Price trend</span><b>${{d.trend}}</b></div>
#           <div class="row"><span class="lbl">Weather (3-day)</span><b>${{d.weather}}</b></div>
#           ${{Object.entries(d.all_prices).map(([m,p])=>`<div class="row"><span class="lbl">${{m}} today</span><b>₹${{p}}/q</b></div>`).join('')}}
#         `;
#         document.getElementById('res').classList.remove('hidden');
#       }}catch(e){{
#         document.getElementById('err').textContent='Error: '+e.message;
#         document.getElementById('err').classList.remove('hidden');
#       }}finally{{
#         document.getElementById('load').classList.add('hidden');
#         document.getElementById('btn').disabled=false;
#       }}
#     }}
    
#     function toggleChat(){{
#       const body=document.getElementById('chatBody');
#       const toggle=document.getElementById('chatToggle');
#       if(body.classList.contains('open')){{
#         body.classList.remove('open');
#         toggle.textContent='▼';
#       }}else{{
#         body.classList.add('open');
#         toggle.textContent='▲';
#       }}
#     }}
    
#     async function sendChat(){{
#       const input=document.getElementById('chatInput');
#       const btn=document.getElementById('chatBtn');
#       const messages=document.getElementById('chatMessages');
#       const question=input.value.trim();
      
#       if(!question)return;
      
#       // Add user message
#       const userMsg=document.createElement('div');
#       userMsg.className='chat-msg user';
#       userMsg.textContent=question;
#       messages.appendChild(userMsg);
      
#       input.value='';
#       btn.disabled=true;
      
#       // Add loading indicator
#       const loadingMsg=document.createElement('div');
#       loadingMsg.className='chat-msg genie';
#       loadingMsg.textContent='🤔 Thinking...';
#       loadingMsg.id='loadingMsg';
#       messages.appendChild(loadingMsg);
#       messages.scrollTop=messages.scrollHeight;
      
#       try{{
#         const r=await fetch('/chat/ask',{{
#           method:'POST',
#           headers:{{'Content-Type':'application/json'}},
#           body:JSON.stringify({{question}})
#         }});
#         const d=await r.json();
        
#         // Remove loading
#         document.getElementById('loadingMsg').remove();
        
#         if(d.error){{
#           const errMsg=document.createElement('div');
#           errMsg.className='chat-msg error';
#           errMsg.textContent='Error: '+d.error;
#           messages.appendChild(errMsg);
#         }}else{{
#           // Add answer
#           const answerMsg=document.createElement('div');
#           answerMsg.className='chat-msg genie';
#           let content=d.answer||'Query executed successfully.';
          
#           // Add SQL if available
#           if(d.sql){{
#             content+=`<div class="chat-sql">${{d.sql}}</div>`;
#           }}
          
#           // Add data preview if available
#           if(d.data && d.data.length>0){{
#             content+=`<div style="margin-top:8px;font-size:11px;color:#666">Returned ${{d.data.length}} rows</div>`;
#           }}
          
#           answerMsg.innerHTML=content;
#           messages.appendChild(answerMsg);
#         }}
#       }}catch(e){{
#         document.getElementById('loadingMsg').remove();
#         const errMsg=document.createElement('div');
#         errMsg.className='chat-msg error';
#         errMsg.textContent='Error: '+e.message;
#         messages.appendChild(errMsg);
#       }}finally{{
#         btn.disabled=false;
#         messages.scrollTop=messages.scrollHeight;
#       }}
#     }}
#   </script>
# </body>
# </html>"""


# def make_chart(crop: str, mandi: str) -> str:
#     """Generate price chart and return as base64 PNG."""
#     try:
#         today  = pd.Timestamp.today()
#         dates  = pd.date_range(end=today, periods=10, freq="D")
#         base   = PRICE_DATA.get((crop, mandi), {}).get("modal_price", 900)
#         trend  = PRICE_DATA.get((crop, mandi), {}).get("pct_change", 0)
#         prices = [max(200, base - (trend/10)*i + np.random.normal(0, 20))
#                   for i in range(10, 0, -1)]

#         f_start  = today + pd.Timedelta(days=1)
#         f_dates  = pd.date_range(start=f_start, periods=10, freq="D")
#         f_prices = [max(200, prices[-1] + trend*i/10) for i in range(1, 11)]
#         f_lower  = [p - 50 for p in f_prices]
#         f_upper  = [p + 50 for p in f_prices]

#         all_prices = {
#             "Nashik": PRICE_DATA.get((crop, "Nashik"), {}).get("modal_price", 800),
#             "Pune":   PRICE_DATA.get((crop, "Pune"),   {}).get("modal_price", 950),
#         }

#         fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6),
#                                         gridspec_kw={"height_ratios": [2, 1]},
#                                         facecolor="#ffffff")
#         fig.subplots_adjust(hspace=0.45)

#         ax1.plot(dates, prices, color="#1D9E75", linewidth=2.2,
#                  label="Actual price", marker="o", markersize=4)
#         ax1.plot([dates[-1], f_dates[0]], [prices[-1], f_prices[0]],
#                  color="#E24B4A", linewidth=1.5, linestyle="--")
#         ax1.plot(f_dates, f_prices, color="#E24B4A", linewidth=2,
#                  linestyle="--", label="10-day forecast")
#         ax1.fill_between(f_dates, f_lower, f_upper, alpha=0.12, color="#E24B4A")
#         ax1.axvline(dates[-1], color="#888780", linewidth=1, linestyle=":")
#         ax1.set_title(f"{crop} — {mandi} mandi price (₹/quintal)", fontsize=11, pad=8)
#         ax1.set_ylabel("₹ / quintal", fontsize=9)
#         ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
#         ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
#         ax1.tick_params(axis="both", labelsize=8)
#         ax1.legend(fontsize=8, loc="upper right")
#         ax1.grid(axis="y", alpha=0.25, linewidth=0.5)

#         sorted_m = sorted(all_prices.keys())
#         bar_prices = [all_prices[m] for m in sorted_m]
#         colors = ["#1D9E75" if m == mandi else "#B5D4F4" for m in sorted_m]
#         bars = ax2.bar(sorted_m, bar_prices, color=colors, width=0.45, edgecolor="none")
#         for bar, price in zip(bars, bar_prices):
#             ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height()+15,
#                      f"₹{int(price)}", ha="center", va="bottom",
#                      fontsize=9, fontweight="bold")
#         ax2.set_title("Today's price — all mandis", fontsize=10, pad=6)
#         ax2.set_ylabel("₹ / quintal", fontsize=9)
#         ax2.set_ylim(0, max(bar_prices)*1.25 if bar_prices else 1500)
#         ax2.grid(axis="y", alpha=0.2, linewidth=0.5)

#         buf = io.BytesIO()
#         fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
#         buf.seek(0)
#         plt.close(fig)
#         return base64.b64encode(buf.getvalue()).decode()
#     except Exception as e:
#         print(f"[chart] failed: {e}")
#         return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="


# @app.get("/", response_class=HTMLResponse)
# def index():
#     return HTML

# @app.get("/health")
# def health():
#     return {"status": "ok"}

# @app.get("/metrics")
# def metrics():
#     return {"status": "ok"}

# @app.get("/advisory")
# def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "English"):
#     try:
#         d       = PRICE_DATA.get((crop, mandi), {"modal_price": 800, "pct_change": 0.0, "trend": "STABLE"})
#         price   = d["modal_price"]
#         pct     = d["pct_change"]
#         trend   = d["trend"]

#         # Decision logic
#         if trend == "FALLING":
#             decision, confidence = "SELL SOON", "MEDIUM"
#             reason = f"Price is down {abs(pct):.1f}% over 7 days at {mandi}. Sell within 3-5 days."
#         elif trend == "RISING":
#             decision, confidence = "HOLD", "HIGH"
#             reason = f"Price is up {pct:.1f}% over 7 days at {mandi}. Hold for better returns."
#         else:
#             decision, confidence = "MONITOR", "LOW"
#             reason = f"Price is stable at {mandi}. Monitor daily before deciding."

#         adv = ADVISORIES.get(crop, ADVISORIES["Onion"])

#         # Try translation via Sarvam
#         recommendation = reason
#         advisory_text  = adv["text"]
#         if lang != "English":
#             try:
#                 from croppulse.sarvam_client import translate
#                 recommendation = translate(reason, lang)
#                 advisory_text  = translate(adv["text"], lang)
#             except Exception as e:
#                 print(f"[advisory] translation skipped: {e}")

#         chart_b64 = make_chart(crop, mandi)

#         return JSONResponse({
#             "decision":        decision,
#             "confidence":      confidence,
#             "modal_price":     int(price),
#             "pct_change":      f"{pct:+.1f}",
#             "trend":           trend,
#             "weather":         "DRY — 0.0 mm",
#             "recommendation":  recommendation,
#             "advisory_text":   advisory_text,
#             "advisory_source": adv["source"],
#             "chart_b64":       chart_b64,
#             "all_prices":      {
#                 "Nashik": int(PRICE_DATA.get((crop,"Nashik"),{}).get("modal_price",800)),
#                 "Pune":   int(PRICE_DATA.get((crop,"Pune"),{}).get("modal_price",950)),
#             },
#         })

#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": str(exc)}, status_code=200)

# # ── Chat endpoint ─────────────────────────────────────────────────────────────
# @app.post("/chat/ask")
# async def chat_ask(request: ChatRequest):
#     """Ask Genie a question and wait for the result."""
#     try:
#         result = genie.ask_question(request.question, timeout=60)
        
#         response_data = {
#             "conversation_id": result.get("conversation_id", ""),
#             "message_id": result.get("message_id", ""),
#             "status": result.get("status", "success"),
#             "answer": result.get("content", ""),
#         }
        
#         # Extract SQL from attachments if available
#         if "attachments" in result and result["attachments"]:
#             for att in result["attachments"]:
#                 if "sql" in att:
#                     response_data["sql"] = att["sql"]
#                     break
        
#         # Extract data if available
#         if "query_result" in result and result["query_result"]:
#             qr = result["query_result"]
#             if "data" in qr and qr["data"]:
#                 response_data["data"] = qr["data"][:10]  # Limit to 10 rows for UI
        
#         # Add error if present
#         if "error" in result:
#             response_data["error"] = result["error"]
        
#         return JSONResponse(response_data)
        
#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": str(exc)}, status_code=500)


# if __name__ == "__main__":
#     import uvicorn
#     port = int(os.getenv("PORT", 8000))
#     print(f"[app] Starting on http://0.0.0.0:{port}")
#     uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
from __future__ import annotations
import os, sys, io, base64

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

# ── Genie Integration Imports ────────────────────────────────────────────────
try:
    from croppulse.config import GENIE_SPACE_ID
    from croppulse.genie_client import GenieClient
except ImportError:
    print("[app] WARNING: croppulse modules not found. Using Mock GenieClient for testing.")
    GENIE_SPACE_ID = "mock_space_id"
    
    class GenieClient:
        def __init__(self, space_id):
            self.space_id = space_id
        def ask_question(self, question, timeout):
            return {
                "columns": ["crop", "modal_price"],
                "data": [["Saffron", "150000"], ["Vanilla", "95000"]],
                "row_count": 2,
                "sql": "SELECT crop, modal_price FROM prices ORDER BY modal_price DESC LIMIT 2",
                "description": f"Mock result for: '{question}'",
                "error": None,
            }

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── Initialize Genie client ──────────────────────────────────────────────────
genie = GenieClient(space_id=GENIE_SPACE_ID)
print(f"[app] Genie client initialized with space: {GENIE_SPACE_ID}")

class ChatRequest(BaseModel):
    question: str

CROPS  = ["Onion", "Tomato"]
MANDIS = ["Nashik", "Pune"]
LANGS  = ["English", "Hindi", "Marathi", "Telugu", "Tamil", "Kannada", "Gujarati"]

# ── Realistic mock data per crop/mandi ────────────────────────────────────────
PRICE_DATA = {
    ("Onion",  "Nashik"): {"modal_price": 820,  "pct_change": -12.8, "trend": "FALLING"},
    ("Onion",  "Pune"):   {"modal_price": 1100, "pct_change":  +4.8, "trend": "RISING"},
    ("Tomato", "Nashik"): {"modal_price": 750,  "pct_change":  +7.1, "trend": "RISING"},
    ("Tomato", "Pune"):   {"modal_price": 900,  "pct_change":  -6.3, "trend": "FALLING"},
}

ADVISORIES = {
    "Onion": {
        "text": "Kharif onion should be sold within 10 days of harvest when ambient humidity exceeds 75%. Prolonged storage under wet conditions increases rotting losses by 30-40%. Monitor APMC prices at Nashik and Pune daily before committing to a sale.",
        "source": "Source: ICAR Onion Post-Harvest Advisory | Page 4",
    },
    "Tomato": {
        "text": "Tomato prices are highly sensitive to rainfall. A 10mm rainfall event typically causes a 15-20% price drop within 5 days. Sell within 3-4 days of harvest for table grade produce.",
        "source": "Source: ICAR Tomato Pest Management Guide | Page 12",
    },
}

CROP_OPTIONS  = "".join(f"<option>{c}</option>" for c in CROPS)
MANDI_OPTIONS = "".join(f"<option>{m}</option>" for m in MANDIS)
LANG_OPTIONS  = "".join(f"<option>{l}</option>" for l in LANGS)

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>AgriBuddy — AI Advisor for Indian Farmers</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7f7f5;color:#1a1a1a}}
    .hdr{{background:#1D9E75;color:#fff;padding:20px 32px}}
    .hdr h1{{font-size:22px;font-weight:600}}
    .hdr p{{font-size:13px;opacity:.85;margin-top:4px}}
    .wrap{{max-width:900px;margin:28px auto;padding:0 18px}}
    .card{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;padding:22px;margin-bottom:18px}}
    .inputs{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}}
    label{{font-size:11px;font-weight:600;color:#666;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}}
    select{{width:100%;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px;background:#fff}}
    .btn{{width:100%;padding:13px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:500;cursor:pointer}}
    .btn:hover{{background:#178a65}}
    .btn:disabled{{background:#aaa;cursor:not-allowed}}
    .badge{{display:inline-block;padding:7px 20px;border-radius:8px;font-size:20px;font-weight:700;color:#fff;margin-bottom:12px}}
    .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
    .row{{display:flex;justify-content:space-between;padding:7px 0;border-bottom:.5px solid #f0f0f0;font-size:13px}}
    .lbl{{color:#888}}
    .adv{{background:#E1F5EE;border-left:3px solid #1D9E75;border-radius:0 8px 8px 0;padding:12px 14px;margin-top:14px}}
    .adv-lbl{{font-size:10px;font-weight:700;color:#085041;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}}
    .adv-txt{{font-size:13px;color:#0F6E56;line-height:1.6}}
    .adv-src{{font-size:11px;color:#1D9E75;margin-top:5px}}
    .rec{{background:#f9f9f7;border-radius:8px;padding:13px;font-size:14px;line-height:1.7;margin-top:13px}}
    img.chart{{width:100%;border-radius:8px}}
    .loading{{text-align:center;padding:36px;color:#888;font-size:14px}}
    .err{{background:#fff0f0;border:.5px solid #fcc;border-radius:8px;padding:13px;color:#c00;font-size:13px}}
    .hidden{{display:none}}
    .conf{{font-size:13px;color:#888;margin-bottom:14px}}
    .foot{{text-align:center;font-size:11px;color:#bbb;padding:20px 0}}
    
    /* Chat panel */
    .chat-panel{{background:#fff;border-radius:12px;border:.5px solid #e0e0e0;margin-top:18px;overflow:hidden}}
    .chat-header{{background:#1D9E75;color:#fff;padding:14px 18px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}}
    .chat-header h3{{font-size:16px;font-weight:600}}
    .chat-toggle{{font-size:18px}}
    .chat-body{{padding:18px;max-height:500px;overflow-y:auto;display:none}}
    .chat-body.open{{display:block}}
    .chat-messages{{margin-bottom:14px;min-height:200px}}
    .chat-msg{{margin-bottom:12px;padding:10px 12px;border-radius:8px;font-size:13px;line-height:1.5}}
    .chat-msg.user{{background:#E1F5EE;text-align:right}}
    .chat-msg.genie{{background:#f5f5f5}}
    .chat-msg.error{{background:#fff0f0;color:#c00}}
    .chat-input-area{{display:flex;gap:8px}}
    .chat-input{{flex:1;padding:10px 12px;border:.5px solid #ccc;border-radius:8px;font-size:14px}}
    .chat-btn{{padding:10px 20px;background:#1D9E75;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer}}
    .chat-btn:hover{{background:#178a65}}
    .chat-btn:disabled{{background:#aaa;cursor:not-allowed}}
    .chat-sql{{background:#f8f8f8;padding:8px;border-radius:4px;font-family:monospace;font-size:11px;margin-top:6px;overflow-x:auto;white-space:pre-wrap;word-break:break-all}}
    .chat-table{{width:100%;border-collapse:collapse;margin-top:8px;font-size:12px}}
    .chat-table th{{background:#1D9E75;color:#fff;padding:6px 10px;text-align:left;font-weight:600;font-size:11px;text-transform:uppercase}}
    .chat-table td{{padding:6px 10px;border-bottom:1px solid #eee}}
    .chat-table tr:nth-child(even){{background:#f9f9f7}}
    .chat-desc{{font-size:13px;color:#333;margin-bottom:6px;font-weight:500}}
    
    @media(max-width:600px){{.inputs,.grid2{{grid-template-columns:1fr}}}}
  </style>
</head>
<body>
  <div class="hdr">
    <h1>CropPulse — AI Advisor for Indian Farmers</h1>
    <p>Data-driven SELL or HOLD recommendations · Mandi prices · Weather · ICAR advisories</p>
  </div>
  <div class="wrap">
    <div class="card">
      <div class="inputs">
        <div><label>Crop</label><select id="crop">{CROP_OPTIONS}</select></div>
        <div><label>Home Mandi</label><select id="mandi">{MANDI_OPTIONS}</select></div>
        <div><label>Language</label><select id="lang">{LANG_OPTIONS}</select></div>
      </div>
      <button class="btn" id="btn" onclick="go()">Get Advisory</button>
    </div>
    <div id="load" class="card loading hidden">Fetching prices, weather and advisory…</div>
    <div id="err" class="err hidden"></div>
    <div id="res" class="hidden">
      <div class="grid2">
        <div class="card">
          <div id="badge" class="badge"></div>
          <div id="conf" class="conf"></div>
          <div id="rows"></div>
          <div class="adv">
            <div class="adv-lbl">Government Advisory (ICAR)</div>
            <div id="atxt" class="adv-txt"></div>
            <div id="asrc" class="adv-src"></div>
          </div>
          <div id="rec" class="rec"></div>
        </div>
        <div class="card">
          <div style="font-size:12px;color:#888;margin-bottom:8px">Past 10 days · 10-day forecast · Mandi comparison</div>
          <img id="chart" class="chart" src="" alt="Price chart">
        </div>
      </div>
    </div>
    
    <div class="chat-panel">
      <div class="chat-header" onclick="toggleChat()">
        <h3>💬 Ask Questions About Mandi Prices</h3>
        <span class="chat-toggle" id="chatToggle">▼</span>
      </div>
      <div class="chat-body" id="chatBody">
        <div class="chat-messages" id="chatMessages">
          <div class="chat-msg genie">
            Hi! Ask me anything about mandi prices, crops, and market trends. For example: "What are the average onion prices in Pune?" or "Show me price trends for tomatoes"
          </div>
        </div>
        <div class="chat-input-area">
          <input type="text" id="chatInput" class="chat-input" placeholder="Ask about mandi prices..." onkeypress="if(event.key==='Enter')sendChat()">
          <button class="chat-btn" id="chatBtn" onclick="sendChat()">Send</button>
        </div>
      </div>
    </div>
    
  </div>
  <div class="foot">APMC Agmarknet · OpenWeatherMap · ICAR India · Sarvam Mayura · Databricks Llama 4 Maverick · Genie AI</div>
  <script>
    const C={{"SELL NOW":"#E24B4A","SELL SOON":"#EF9F27","HOLD":"#1D9E75","HOLD SHORT":"#378ADD","MONITOR":"#888780"}};
    async function go(){{
      const crop=document.getElementById('crop').value,
            mandi=document.getElementById('mandi').value,
            lang=document.getElementById('lang').value;
      document.getElementById('btn').disabled=true;
      document.getElementById('load').classList.remove('hidden');
      document.getElementById('res').classList.add('hidden');
      document.getElementById('err').classList.add('hidden');
      try{{
        const r=await fetch(`/advisory?crop=${{encodeURIComponent(crop)}}&mandi=${{encodeURIComponent(mandi)}}&lang=${{encodeURIComponent(lang)}}`);
        const d=await r.json();
        if(d.error)throw new Error(d.error);
        const col=C[d.decision]||'#888';
        document.getElementById('badge').textContent=d.decision;
        document.getElementById('badge').style.background=col;
        document.getElementById('conf').textContent='Confidence: '+d.confidence;
        document.getElementById('atxt').textContent=d.advisory_text;
        document.getElementById('asrc').textContent=d.advisory_source;
        document.getElementById('rec').textContent=d.recommendation;
        document.getElementById('chart').src='data:image/png;base64,'+d.chart_b64;
        document.getElementById('rows').innerHTML=`
          <div class="row"><span class="lbl">Today's price</span><b>₹${{d.modal_price}}/quintal</b></div>
          <div class="row"><span class="lbl">7-day change</span><b style="color:${{col}}">${{d.pct_change}}%</b></div>
          <div class="row"><span class="lbl">Price trend</span><b>${{d.trend}}</b></div>
          <div class="row"><span class="lbl">Weather (3-day)</span><b>${{d.weather}}</b></div>
          ${{Object.entries(d.all_prices).map(([m,p])=>`<div class="row"><span class="lbl">${{m}} today</span><b>₹${{p}}/q</b></div>`).join('')}}
        `;
        document.getElementById('res').classList.remove('hidden');
      }}catch(e){{
        document.getElementById('err').textContent='Error: '+e.message;
        document.getElementById('err').classList.remove('hidden');
      }}finally{{
        document.getElementById('load').classList.add('hidden');
        document.getElementById('btn').disabled=false;
      }}
    }}
    
    function toggleChat(){{
      const body=document.getElementById('chatBody');
      const toggle=document.getElementById('chatToggle');
      if(body.classList.contains('open')){{
        body.classList.remove('open');
        toggle.textContent='▼';
      }}else{{
        body.classList.add('open');
        toggle.textContent='▲';
      }}
    }}
    
    function buildTable(columns, data) {{
      if (!columns || !data || data.length===0) return '';
      let h='<table class="chat-table"><thead><tr>';
      for (const c of columns) h+=`<th>${{c}}</th>`;
      h+='</tr></thead><tbody>';
      for (const row of data) {{
        h+='<tr>';
        for (const v of row) h+=`<td>${{v!==null&&v!==undefined?v:'—'}}</td>`;
        h+='</tr>';
      }}
      h+='</tbody></table>';
      return h;
    }}
    
    async function sendChat(){{
      const input=document.getElementById('chatInput');
      const btn=document.getElementById('chatBtn');
      const messages=document.getElementById('chatMessages');
      const question=input.value.trim();
      if(!question) return;
      
      const userMsg=document.createElement('div');
      userMsg.className='chat-msg user';
      userMsg.textContent=question;
      messages.appendChild(userMsg);
      input.value='';
      btn.disabled=true;
      
      const loadingMsg=document.createElement('div');
      loadingMsg.className='chat-msg genie';
      loadingMsg.textContent='🤔 Thinking...';
      loadingMsg.id='loadingMsg';
      messages.appendChild(loadingMsg);
      messages.scrollTop=messages.scrollHeight;
      
      try{{
        const r=await fetch('/chat/ask',{{
          method:'POST',
          headers:{{'Content-Type':'application/json'}},
          body:JSON.stringify({{question}})
        }});
        const d=await r.json();
        document.getElementById('loadingMsg').remove();
        
        if(d.error){{
          const e=document.createElement('div');
          e.className='chat-msg error';
          e.textContent='Error: '+d.error;
          messages.appendChild(e);
        }}else{{
          const ans=document.createElement('div');
          ans.className='chat-msg genie';
          let html='';
          
          // 1) Description / natural-language answer
          if(d.description) html+=`<div class="chat-desc">${{d.description}}</div>`;
          
          // 2) Data table
          if(d.columns && d.columns.length>0 && d.data && d.data.length>0){{
            html+=buildTable(d.columns, d.data);
            html+=`<div style="margin-top:6px;font-size:11px;color:#888">${{d.row_count||d.data.length}} row(s)</div>`;
          }} else if(!d.description){{
            html+='<span style="color:#888">No results found.</span>';
          }}
          
          // 3) Collapsible SQL
          if(d.sql){{
            html+=`<details style="margin-top:8px"><summary style="font-size:11px;color:#888;cursor:pointer">Show SQL</summary><div class="chat-sql">${{d.sql}}</div></details>`;
          }}
          
          ans.innerHTML=html;
          messages.appendChild(ans);
        }}
      }}catch(e){{
        document.getElementById('loadingMsg')?.remove();
        const err=document.createElement('div');
        err.className='chat-msg error';
        err.textContent='Error: '+e.message;
        messages.appendChild(err);
      }}finally{{
        btn.disabled=false;
        messages.scrollTop=messages.scrollHeight;
      }}
    }}
  </script>
</body>
</html>"""


def make_chart(crop: str, mandi: str) -> str:
    try:
        today  = pd.Timestamp.today()
        dates  = pd.date_range(end=today, periods=10, freq="D")
        base   = PRICE_DATA.get((crop, mandi), {}).get("modal_price", 900)
        trend  = PRICE_DATA.get((crop, mandi), {}).get("pct_change", 0)
        prices = [max(200, base - (trend/10)*i + np.random.normal(0, 20))
                  for i in range(10, 0, -1)]
        f_start  = today + pd.Timedelta(days=1)
        f_dates  = pd.date_range(start=f_start, periods=10, freq="D")
        f_prices = [max(200, prices[-1] + trend*i/10) for i in range(1, 11)]
        f_lower  = [p - 50 for p in f_prices]
        f_upper  = [p + 50 for p in f_prices]
        all_prices = {
            "Nashik": PRICE_DATA.get((crop, "Nashik"), {}).get("modal_price", 800),
            "Pune":   PRICE_DATA.get((crop, "Pune"),   {}).get("modal_price", 950),
        }
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6),
                                        gridspec_kw={"height_ratios": [2, 1]},
                                        facecolor="#ffffff")
        fig.subplots_adjust(hspace=0.45)
        ax1.plot(dates, prices, color="#1D9E75", linewidth=2.2,
                 label="Actual price", marker="o", markersize=4)
        ax1.plot([dates[-1], f_dates[0]], [prices[-1], f_prices[0]],
                 color="#E24B4A", linewidth=1.5, linestyle="--")
        ax1.plot(f_dates, f_prices, color="#E24B4A", linewidth=2,
                 linestyle="--", label="10-day forecast")
        ax1.fill_between(f_dates, f_lower, f_upper, alpha=0.12, color="#E24B4A")
        ax1.axvline(dates[-1], color="#888780", linewidth=1, linestyle=":")
        ax1.set_title(f"{crop} — {mandi} mandi price (₹/quintal)", fontsize=11, pad=8)
        ax1.set_ylabel("₹ / quintal", fontsize=9)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
        ax1.tick_params(axis="both", labelsize=8)
        ax1.legend(fontsize=8, loc="upper right")
        ax1.grid(axis="y", alpha=0.25, linewidth=0.5)
        sorted_m = sorted(all_prices.keys())
        bar_prices = [all_prices[m] for m in sorted_m]
        colors = ["#1D9E75" if m == mandi else "#B5D4F4" for m in sorted_m]
        bars = ax2.bar(sorted_m, bar_prices, color=colors, width=0.45, edgecolor="none")
        for bar, price in zip(bars, bar_prices):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height()+15,
                     f"₹{int(price)}", ha="center", va="bottom",
                     fontsize=9, fontweight="bold")
        ax2.set_title("Today's price — all mandis", fontsize=10, pad=6)
        ax2.set_ylabel("₹ / quintal", fontsize=9)
        ax2.set_ylim(0, max(bar_prices)*1.25 if bar_prices else 1500)
        ax2.grid(axis="y", alpha=0.2, linewidth=0.5)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
        buf.seek(0)
        plt.close(fig)
        return base64.b64encode(buf.getvalue()).decode()
    except Exception as e:
        print(f"[chart] failed: {e}")
        return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics")
def metrics():
    return {"status": "ok"}

@app.get("/advisory")
def advisory(crop: str = "Onion", mandi: str = "Nashik", lang: str = "English"):
    try:
        d       = PRICE_DATA.get((crop, mandi), {"modal_price": 800, "pct_change": 0.0, "trend": "STABLE"})
        price   = d["modal_price"]
        pct     = d["pct_change"]
        trend   = d["trend"]
        if trend == "FALLING":
            decision, confidence = "SELL SOON", "MEDIUM"
            reason = f"Price is down {abs(pct):.1f}% over 7 days at {mandi}. Sell within 3-5 days."
        elif trend == "RISING":
            decision, confidence = "HOLD", "HIGH"
            reason = f"Price is up {pct:.1f}% over 7 days at {mandi}. Hold for better returns."
        else:
            decision, confidence = "MONITOR", "LOW"
            reason = f"Price is stable at {mandi}. Monitor daily before deciding."
        adv = ADVISORIES.get(crop, ADVISORIES["Onion"])
        recommendation = reason
        advisory_text  = adv["text"]
        if lang != "English":
            try:
                from croppulse.sarvam_client import translate
                recommendation = translate(reason, lang)
                advisory_text  = translate(adv["text"], lang)
            except Exception as e:
                print(f"[advisory] translation skipped: {e}")
        chart_b64 = make_chart(crop, mandi)
        return JSONResponse({
            "decision":        decision,
            "confidence":      confidence,
            "modal_price":     int(price),
            "pct_change":      f"{pct:+.1f}",
            "trend":           trend,
            "weather":         "DRY — 0.0 mm",
            "recommendation":  recommendation,
            "advisory_text":   advisory_text,
            "advisory_source": adv["source"],
            "chart_b64":       chart_b64,
            "all_prices":      {
                "Nashik": int(PRICE_DATA.get((crop,"Nashik"),{}).get("modal_price",800)),
                "Pune":   int(PRICE_DATA.get((crop,"Pune"),{}).get("modal_price",950)),
            },
        })
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(exc)}, status_code=200)


# ── Chat endpoint ─────────────────────────────────────────────────────────────
@app.post("/chat/ask")
async def chat_ask(request: ChatRequest):
    """Ask Genie a question → return columns + data for the frontend table."""
    try:
        result = genie.ask_question(request.question, timeout=60)

        # genie_client.ask_question() returns:
        #   columns, data, row_count, sql, description, error

        if result.get("error"):
            return JSONResponse({"error": result["error"]}, status_code=200)

        return JSONResponse({
            "columns":     result.get("columns", []),
            "data":        result.get("data", [])[:20],
            "row_count":   result.get("row_count", 0),
            "sql":         result.get("sql", ""),
            "description": result.get("description", ""),
        })

    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(exc)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"[app] Starting on http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
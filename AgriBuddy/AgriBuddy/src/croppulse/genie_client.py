# """
# genie_client.py — Client for Databricks Genie Conversation API.
# Uses w.api_client.do() so auth is handled automatically (works with OAuth in Databricks Apps).
# """
# from __future__ import annotations
# import time
# from typing import Dict, Any
# from databricks.sdk import WorkspaceClient


# class GenieClient:
#     """Client for interacting with Genie Conversation API."""

#     def __init__(self, space_id: str):
#         self.space_id = space_id
#         self.w        = WorkspaceClient()
#         print(f"[genie] Initialized. space={self.space_id}")

#     def _post(self, path: str, body: dict) -> dict:
#         return self.w.api_client.do(
#             "POST",
#             f"/api/2.0/genie/spaces/{self.space_id}{path}",
#             body=body,
#         )

#     def _get(self, path: str) -> dict:
#         return self.w.api_client.do(
#             "GET",
#             f"/api/2.0/genie/spaces/{self.space_id}{path}",
#         )

#     def start_conversation(self, question: str) -> Dict[str, Any]:
#         print(f"[genie] Starting conversation: {question[:80]}")
#         data = self._post("/start-conversation", {"content": question})
#         print(f"[genie] response keys: {list(data.keys())}")
#         print(f"[genie] full response: {data}")

#         conversation_id = data["conversation"]["id"]
#         message_id      = data["message"]["id"]
#         status          = data.get("message", {}).get("status", "EXECUTING_QUERY")

#         return {"conversation_id": conversation_id, "message_id": message_id, "status": status}

#     def get_message_status(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
#         data = self._get(f"/conversations/{conversation_id}/messages/{message_id}")
#         print(f"[genie] message status={data.get('status')}")

#         result = {"status": data.get("status", "UNKNOWN"), "content": data.get("content", "")}

#         if "attachments" in data:
#             result["attachments"] = []
#             for att in data["attachments"]:
#                 if "query" in att:
#                     result["attachments"].append({
#                         "sql": att["query"].get("query", ""),
#                         "description": att["query"].get("description", ""),
#                     })

#         if "query_result" in data and data["query_result"]:
#             qr = data["query_result"]
#             result["query_result"] = {"row_count": qr.get("row_count", 0), "data": qr.get("data_array")}

#         if data.get("error"):
#             result["error"] = str(data["error"])

#         return result

#     def wait_for_result(self, conversation_id: str, message_id: str, timeout: int = 60, poll_interval: float = 2.0) -> Dict[str, Any]:
#         start = time.time()
#         while True:
#             if time.time() - start > timeout:
#                 raise TimeoutError(f"Genie query timed out after {timeout}s")
#             result = self.get_message_status(conversation_id, message_id)
#             if result["status"] in ("COMPLETED", "FAILED", "CANCELLED", "ERROR"):
#                 data = result.get("query_result", {}).get("data", [])
#                 return {"data": data, "error": result.get("error")}
#             time.sleep(poll_interval)

#     def ask_question(self, question: str, timeout: int = 60) -> Dict[str, Any]:
#         try:
#             conv = self.start_conversation(question)
#             return self.wait_for_result(conv["conversation_id"], conv["message_id"], timeout=timeout)
#         except Exception as e:
#             import traceback
#             traceback.print_exc()
#             raise Exception(f"Failed to start Genie conversation: {str(e)}")



# ### final version 
# """
# genie_client.py — Client for Databricks Genie Conversation API.
# Uses w.api_client.do() so auth is handled automatically (works with OAuth in Databricks Apps).
# """
# from __future__ import annotations
# import time
# from typing import Dict, Any, List, Optional
# from databricks.sdk import WorkspaceClient


# class GenieClient:
#     """Client for interacting with Genie Conversation API."""

#     def __init__(self, space_id: str):
#         self.space_id = space_id
#         self.w        = WorkspaceClient()
#         print(f"[genie] Initialized. space={self.space_id}")

#     def _post(self, path: str, body: dict) -> dict:
#         return self.w.api_client.do(
#             "POST",
#             f"/api/2.0/genie/spaces/{self.space_id}{path}",
#             body=body,
#         )

#     def _get(self, path: str) -> dict:
#         return self.w.api_client.do(
#             "GET",
#             f"/api/2.0/genie/spaces/{self.space_id}{path}",
#         )

#     # ------------------------------------------------------------------ #
#     #  Conversation lifecycle                                             #
#     # ------------------------------------------------------------------ #
#     def start_conversation(self, question: str) -> Dict[str, Any]:
#         print(f"[genie] Starting conversation: {question[:80]}")
#         data = self._post("/start-conversation", {"content": question})

#         conversation_id = data["conversation"]["id"]
#         message_id      = data["message"]["id"]
#         status          = data.get("message", {}).get("status", "EXECUTING_QUERY")

#         return {
#             "conversation_id": conversation_id,
#             "message_id": message_id,
#             "status": status,
#         }

#     # ------------------------------------------------------------------ #
#     #  Poll message status                                                #
#     # ------------------------------------------------------------------ #
#     def get_message_status(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
#         data = self._get(f"/conversations/{conversation_id}/messages/{message_id}")
#         print(f"[genie] message status={data.get('status')}")

#         result: Dict[str, Any] = {
#             "status": data.get("status", "UNKNOWN"),
#             "content": data.get("content", ""),
#         }

#         # Capture SQL from attachments so we can fetch its result later
#         if "attachments" in data:
#             result["attachments"] = []
#             for att in data["attachments"]:
#                 att_info: Dict[str, Any] = {}
#                 if "query" in att:
#                     att_info["sql"]         = att["query"].get("query", "")
#                     att_info["description"] = att["query"].get("description", "")
#                 if "id" in att:
#                     att_info["attachment_id"] = att["id"]
#                 if att_info:
#                     result["attachments"].append(att_info)

#         # Some API versions inline the text reply
#         if "text" in data:
#             result["text"] = data["text"]

#         if data.get("error"):
#             result["error"] = str(data["error"])

#         return result

#     # ------------------------------------------------------------------ #
#     #  Fetch the actual query-result rows + columns                       #
#     # ------------------------------------------------------------------ #
#     def get_query_result(
#         self, conversation_id: str, message_id: str, attachment_id: Optional[str] = None
#     ) -> Dict[str, Any]:
#         """
#         Fetch query result via the dedicated endpoint.
#         Returns {"columns": [...], "data": [[...], ...], "row_count": int}
#         """
#         # Primary path: per-attachment query result
#         if attachment_id:
#             path = (
#                 f"/conversations/{conversation_id}"
#                 f"/messages/{message_id}"
#                 f"/attachments/{attachment_id}"
#                 f"/query-result"
#             )
#         else:
#             # Fallback: message-level query result
#             path = (
#                 f"/conversations/{conversation_id}"
#                 f"/messages/{message_id}"
#                 f"/query-result"
#             )

#         try:
#             qr = self._get(path)
#         except Exception as e:
#             print(f"[genie] query-result fetch failed: {e}")
#             return {"columns": [], "data": [], "row_count": 0}

#         columns = [
#             col.get("name", f"col_{i}")
#             for i, col in enumerate(qr.get("manifest", {}).get("schema", {}).get("columns", []))
#         ]
#         rows      = qr.get("data_array", [])
#         row_count = qr.get("row_count", len(rows))

#         return {"columns": columns, "data": rows, "row_count": row_count}

#     # ------------------------------------------------------------------ #
#     #  Wait for completion, then pull the result                          #
#     # ------------------------------------------------------------------ #
#     def wait_for_result(
#         self,
#         conversation_id: str,
#         message_id: str,
#         timeout: int = 60,
#         poll_interval: float = 2.0,
#     ) -> Dict[str, Any]:
#         start = time.time()
#         last_result: Dict[str, Any] = {}

#         while True:
#             if time.time() - start > timeout:
#                 raise TimeoutError(f"Genie query timed out after {timeout}s")

#             last_result = self.get_message_status(conversation_id, message_id)
#             status = last_result["status"]

#             if status in ("COMPLETED", "FAILED", "CANCELLED", "ERROR"):
#                 break
#             time.sleep(poll_interval)

#         # ---- Handle non-success statuses ---- #
#         if last_result["status"] != "COMPLETED":
#             return {
#                 "columns": [],
#                 "data": [],
#                 "row_count": 0,
#                 "description": "",
#                 "sql": "",
#                 "error": last_result.get("error", f"Query ended with status: {status}"),
#             }

#         # ---- Fetch query result from the dedicated endpoint ---- #
#         attachment_id = None
#         sql           = ""
#         description   = ""

#         attachments = last_result.get("attachments", [])
#         if attachments:
#             attachment_id = attachments[0].get("attachment_id")
#             sql           = attachments[0].get("sql", "")
#             description   = attachments[0].get("description", "")

#         qr = self.get_query_result(conversation_id, message_id, attachment_id)

#         return {
#             "columns":     qr["columns"],
#             "data":        qr["data"],
#             "row_count":   qr["row_count"],
#             "sql":         sql,
#             "description": description,
#             "error":       last_result.get("error"),
#         }

#     # ------------------------------------------------------------------ #
#     #  High-level: ask a question and get a formatted answer              #
#     # ------------------------------------------------------------------ #
#     def ask_question(self, question: str, timeout: int = 60) -> Dict[str, Any]:
#         try:
#             conv   = self.start_conversation(question)
#             result = self.wait_for_result(
#                 conv["conversation_id"], conv["message_id"], timeout=timeout
#             )
#             return result
#         except Exception as e:
#             import traceback
#             traceback.print_exc()
#             raise Exception(f"Failed to query Genie: {e}")

#     # ------------------------------------------------------------------ #
#     #  Utility: turn result into a readable string                        #
#     # ------------------------------------------------------------------ #
#     @staticmethod
#     def format_result(result: Dict[str, Any]) -> str:
#         """Return a human-readable string from an ask_question result."""
#         if result.get("error"):
#             return f"Error: {result['error']}"

#         columns = result.get("columns", [])
#         rows    = result.get("data", [])

#         if not rows:
#             return "Query returned no results."

#         if result.get("description"):
#             lines = [result["description"], ""]
#         else:
#             lines = []

#         # Header
#         lines.append(" | ".join(columns))
#         lines.append("-+-".join("-" * max(len(c), 10) for c in columns))

#         # Rows (cap at 50 for display)
#         for row in rows[:50]:
#             lines.append(" | ".join(str(v) if v is not None else "" for v in row))

#         if len(rows) > 50:
#             lines.append(f"... and {len(rows) - 50} more rows")

#         return "\n".join(lines)

"""
genie_client.py — Client for Databricks Genie Conversation API.
Docs: https://docs.databricks.com/aws/en/genie/conversation-api

API flow:
  1. POST /start-conversation          → conversation_id, message_id (status=IN_PROGRESS)
  2. GET  /messages/{msg_id}            → poll until status=COMPLETED; attachments[] populated
  3. GET  /messages/{msg_id}/query-result/{attachment_id}  → columns + rows
"""
from __future__ import annotations
import time
import json
from typing import Dict, Any, Optional
from databricks.sdk import WorkspaceClient


class GenieClient:

    def __init__(self, space_id: str):
        self.space_id = space_id
        self.w        = WorkspaceClient()
        print(f"[genie] Initialized. space={self.space_id}")

    # ── low-level helpers ────────────────────────────────────────────────
    def _post(self, path: str, body: dict) -> dict:
        return self.w.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{self.space_id}{path}",
            body=body,
        )

    def _get(self, path: str) -> dict:
        return self.w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}{path}",
        )

    # ── Step 1: start conversation ───────────────────────────────────────
    def start_conversation(self, question: str) -> Dict[str, Any]:
        print(f"[genie] Starting conversation: {question[:80]}")
        data = self._post("/start-conversation", {"content": question})

        conversation_id = data["conversation"]["id"]
        message_id      = data["message"]["id"]
        status          = data.get("message", {}).get("status", "IN_PROGRESS")

        return {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "status": status,
        }

    # ── Step 2: poll message until terminal status ───────────────────────
    def get_message(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
        """GET /conversations/{conv}/messages/{msg} — returns full message."""
        data = self._get(f"/conversations/{conversation_id}/messages/{message_id}")
        print(f"[genie] poll status={data.get('status')}  keys={list(data.keys())}")
        return data

    # ── Step 3: fetch query result rows ──────────────────────────────────
    def get_query_result(
        self, conversation_id: str, message_id: str, attachment_id: str
    ) -> Dict[str, Any]:
        """
        GET /conversations/{conv}/messages/{msg}/query-result/{att}
        Returns {"columns": [...], "data": [[...], ...], "row_count": int}
        """
        path = (
            f"/conversations/{conversation_id}"
            f"/messages/{message_id}"
            f"/query-result/{attachment_id}"
        )
        print(f"[genie] fetching query-result: {path}")
        raw = self._get(path)
        print(f"[genie] query-result top keys: {list(raw.keys())}")

        # Databricks wraps the result in "statement_response"
        qr = raw.get("statement_response", raw)
        print(f"[genie] unwrapped keys: {list(qr.keys())}")

        # ── extract columns from manifest.schema.columns ──
        columns = []
        manifest = qr.get("manifest", {})
        schema   = manifest.get("schema", manifest)  # some versions skip "schema"
        for i, col in enumerate(schema.get("columns", [])):
            columns.append(col.get("name", f"col_{i}"))

        # ── extract rows from result.data_array ──
        result_obj = qr.get("result", qr)
        rows = result_obj.get("data_array", result_obj.get("data", []))
        row_count = (
            manifest.get("total_row_count")
            or result_obj.get("row_count")
            or len(rows)
        )
        print(f"[genie] got {len(columns)} columns, {row_count} rows")

        return {"columns": columns, "data": rows, "row_count": row_count}

    # ── Combined: poll → extract attachments → fetch result ──────────────
    def wait_for_result(
        self,
        conversation_id: str,
        message_id: str,
        timeout: int = 60,
        poll_interval: float = 2.0,
    ) -> Dict[str, Any]:
        start = time.time()
        msg: Dict[str, Any] = {}

        # ---- poll until terminal status ----
        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"Genie timed out after {timeout}s")
            msg = self.get_message(conversation_id, message_id)
            status = msg.get("status", "UNKNOWN")
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                break
            time.sleep(poll_interval)

        if msg.get("status") != "COMPLETED":
            return {
                "columns": [], "data": [], "row_count": 0,
                "sql": "", "description": "",
                "error": msg.get("error") or f"Query ended with status: {msg.get('status')}",
            }

        # ---- parse attachments (populated on COMPLETED) ----
        attachments  = msg.get("attachments") or []
        sql          = ""
        description  = ""
        attachment_id = None

        print(f"[genie] {len(attachments)} attachment(s)")
        text_answer = ""
        for att in attachments:
            print(f"[genie]   attachment keys: {list(att.keys())}")
            # Extract SQL + attachment_id only from the query attachment
            if "query" in att:
                q = att["query"]
                sql         = q.get("query", "")
                description = q.get("description", "")
                # The attachment_id for fetching query results lives HERE
                attachment_id = att.get("attachment_id") or att.get("id")
            # Natural-language answer is in a separate text-only attachment
            if "text" in att:
                text_answer = att["text"]

        # Prefer the NL text answer over the SQL description for display
        if text_answer:
            description = text_answer

        # ---- fetch query result if we have an attachment_id ----
        if attachment_id:
            try:
                qr = self.get_query_result(conversation_id, message_id, attachment_id)
                return {
                    "columns":     qr["columns"],
                    "data":        qr["data"],
                    "row_count":   qr["row_count"],
                    "sql":         sql,
                    "description": description,
                    "error":       None,
                }
            except Exception as e:
                print(f"[genie] query-result fetch failed: {e}")
                # Return what we have (SQL + description) with the error
                return {
                    "columns": [], "data": [], "row_count": 0,
                    "sql": sql, "description": description,
                    "error": f"Query completed but failed to fetch results: {e}",
                }

        # ---- no attachment with a query (text-only answer) ----
        # Some questions get a plain text response with no SQL
        text_answer = ""
        for att in attachments:
            if "text" in att:
                text_answer = att["text"]
                break
        return {
            "columns": [], "data": [], "row_count": 0,
            "sql": "", "description": text_answer or description,
            "error": None,
        }

    # ── Public API ───────────────────────────────────────────────────────
    def ask_question(self, question: str, timeout: int = 60) -> Dict[str, Any]:
        try:
            conv = self.start_conversation(question)
            return self.wait_for_result(
                conv["conversation_id"], conv["message_id"], timeout=timeout
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise Exception(f"Failed to query Genie: {e}") 
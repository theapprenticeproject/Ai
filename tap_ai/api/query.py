"""
Unified query submission endpoint.

Accepts either a text query (`q`) or a voice audio URL (`audio_url`) and
dispatches processing to the appropriate RabbitMQ queue:

    text  →  text_query_queue  →  LLMWorker
    voice →  audio_stt_queue   →  STTWorker → LLMWorker → (optional) TTSWorker

Applies per-user rate limiting (60 req/min text, 30 req/min voice), creates a
unique `request_id`, and returns it immediately. The caller polls
`tap_ai.api.result.result` to retrieve the answer.

Endpoint:
    POST /api/method/tap_ai.api.query.query
    Body (text):  { "q": "...", "user_id": "..." }
    Body (voice): { "audio_url": "...", "user_id": "..." }
    Optional:     "session_id" for multi-turn conversation context
"""

import frappe
import json
import uuid
import time
from loguru import logger
from tap_ai.services.ratelimit import check_rate_limit
from tap_ai.utils.mq import publish_to_queue, MQUnavailableError, MQPublishError


def _close_http_connection() -> None:
    try:
        response_headers = getattr(frappe.local, "response_headers", None)
        if response_headers is None:
            frappe.local.response_headers = []
            response_headers = frappe.local.response_headers

        header = ("Connection", "close")
        if header not in response_headers:
            response_headers.append(header)
    except Exception:
        pass


def _extract_api_key() -> str | None:
    auth = frappe.get_request_header("Authorization") or ""
    if not auth.lower().startswith("token "):
        return None
    try:
        return auth.split()[1].split(":")[0]
    except Exception:
        return None


def _resolve_user_id(data: dict) -> str:
    # Keep compatibility with existing clients, but avoid trusting Guest placeholders.
    user_id = data.get("user_id")
    if not user_id or user_id == "Guest":
        user_id = frappe.session.user
    return user_id


@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Unified Query API.
    Accepts either:
    - q (text input), or
    - audio_url (voice input)

    Returns request_id immediately. Processing is handled by RabbitMQ workers.
    """
    try:
        data = frappe.local.form_dict or {}
        q = (data.get("q") or "").strip()
        audio_url = (data.get("audio_url") or "").strip()
        user_id = _resolve_user_id(data)
        session_id = data.get("session_id")
        logger.info(f"Query API entry: q={q!r} user_id={user_id}")

        if not q and not audio_url:
            frappe.throw("Provide one input in POST body: q (text) or audio_url (voice).")
        if q and audio_url:
            frappe.throw("Provide only one input per request: q or audio_url, not both.")

        is_voice = bool(audio_url)

        # Voice requests are costlier, so use a lower limit.
        api_key = _extract_api_key()
        scope = f"voice_api_{user_id}" if is_voice else f"query_api_{user_id}"
        limit = 30 if is_voice else 60

        ok, remaining, reset = check_rate_limit(
            api_key=api_key,
            scope=scope,
            limit=limit,
            window_sec=60
        )
        logger.debug(f"Rate limit check: ok={ok} remaining={remaining}")
        if not ok:
            if is_voice:
                message = f"Voice query rate limit exceeded. Try again in {reset} seconds."
            else:
                message = f"Rate limit exceeded. Try again in {reset} seconds."
            frappe.throw(
                message,
                frappe.TooManyRequestsError,
            )

        request_prefix = "VREQ" if is_voice else "REQ"

        # Create a unique request id for every incoming request
        request_id = f"{request_prefix}_{uuid.uuid4().hex[:8]}"

        state = {
            "status": "pending",
            "user_id": user_id,
            "mode": "voice" if is_voice else "text",
            "request_started_at_ms": int(time.time() * 1000),
        }

        if is_voice:
            state["audio_url"] = audio_url
        else:
            state.update({
                "answer": None,
                "query": q,
                "history": [],
            })

        try:
            frappe.cache().set(request_id, json.dumps(state), ex=3600)
            logger.debug(f"Cache set: request_id={request_id} mode={state.get('mode')}")
        except Exception as e:
            logger.warning(f"Cache set failed for {request_id}: {e}")

        if is_voice:
            payload = {
                "request_id": request_id,
                "audio_url": audio_url,
                "user_id": user_id,
            }
            if session_id:
                payload["session_id"] = session_id
            publish_to_queue("audio_stt_queue", payload)
        else:
            payload = {
                "request_id": request_id,
                "query": q,
                "user_id": user_id,
            }
            if session_id:
                payload["session_id"] = session_id
            publish_to_queue("text_query_queue", payload)

        logger.info(f"Published to queue: request_id={request_id}")
        return {"request_id": request_id, "status": "queued"}

    except frappe.TooManyRequestsError:
        raise
    except MQUnavailableError as e:
        logger.error(f"MQ unavailable: {e}")
        frappe.local.response["http_status_code"] = 503
        frappe.throw(str(e))
    except MQPublishError as e:
        logger.error(f"MQ publish error: {e}")
        frappe.local.response["http_status_code"] = 500
        frappe.throw(str(e))
    except frappe.ValidationError:
        raise
    except Exception as e:
        logger.error(f"Unhandled exception in Query API: {e}")
        try:
            frappe.log_error(str(e), "Query API Error")
        except Exception:
            pass
        frappe.local.response["http_status_code"] = 500
        frappe.throw("An internal error occurred while processing your request.")
    finally:
        _close_http_connection()
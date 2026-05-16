# tap_ai/workers/llm_worker.py

import frappe
import json
import pika
import time
from tap_ai.services.router import (
    process_query,
    choose_tool,
    _get_history_from_cache,
    _save_history_to_cache,
    _append_history_to_db,
)
from tap_ai.services.rag_answerer import (
    retrieve_vector_search,
    synthesize_vector_search_answer,
    _refine_query_with_history,
)
from tap_ai.utils.mq import publish_to_queue


def _tts_enabled_for_voice() -> bool:
    """Feature flag to control whether voice flow should enqueue TTS."""
    # Default OFF to prioritize response latency unless explicitly enabled.
    val = frappe.conf.get("tap_ai_enable_voice_tts", 0)
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _load_request_state(request_id: str) -> dict:
    current_state = frappe.cache().get(request_id)
    return json.loads(current_state) if current_state else {}


def _save_request_state(request_id: str, state_dict: dict) -> None:
    try:
        frappe.cache().set(request_id, json.dumps(state_dict))
        print(f"[LLM Worker] cache.set: request_id={request_id} status={state_dict.get('status')} tool={state_dict.get('tool')} router_decision={state_dict.get('router_decision')} ts={int(time.time())}")
    except Exception as e:
        print(f"[LLM Worker] cache.set failed for {request_id}: {e}")


def _resolve_result_tool(result: dict, fallback_tool: str) -> str:
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    return (
        metadata.get("primary_engine")
        or result.get("response_type")
        or result.get("tool")
        or fallback_tool
    )


def _apply_end_to_end_timing(state_dict: dict, metadata: dict) -> dict:
    started_at_ms = state_dict.get("request_started_at_ms")
    if not started_at_ms:
        return metadata

    total_e2e_ms = int(time.time() * 1000) - int(started_at_ms)
    # Merge worker-side timings (refine_query_ms, route_ms) with process result timings
    state_timings = (state_dict.get("metadata") or {}).get("timings_ms") or {}
    timings_ms = {**state_timings, **dict(metadata.get("timings_ms") or {})}
    processing_ms = timings_ms.get("processing_total") or timings_ms.get("knowledge_bank") or timings_ms.get("direct_llm") or timings_ms.get("total")
    stt_ms = state_dict.get("stt_timing_ms") or timings_ms.get("stt") or 0
    router_ms = state_dict.get("router_timing_ms") or timings_ms.get("route_ms") or timings_ms.get("router_precheck") or 0

    timings_ms["end_to_end"] = total_e2e_ms
    timings_ms["queue_wait"] = max(total_e2e_ms - int(processing_ms or 0) - int(stt_ms or 0) - int(router_ms or 0), 0)
    if processing_ms is not None:
        timings_ms["processing"] = int(processing_ms)
    if stt_ms:
        timings_ms["stt"] = int(stt_ms)
    if router_ms:
        timings_ms["route_ms"] = int(router_ms)
    timings_ms["total"] = total_e2e_ms

    metadata = dict(metadata)
    metadata["timings_ms"] = timings_ms
    return metadata


def _publish_vector_search_synthesis(
    request_id: str,
    query: str,
    user_id: str,
    session_id: str,
    context_text: str,
    language: str = "en",
    is_voice: bool = False,
) -> None:
    publish_to_queue(
        "text_query_queue",
        {
            "request_id": request_id,
            "query": query,
            "user_id": user_id,
            "session_id": session_id,
            "stage": "vector_search_synthesis",
            "context_text": context_text,
            "language": language,
            "is_voice": is_voice,
        },
    )


def _finalize_voice_answer(
    request_id: str,
    answer: str,
    query: str,
    user_id: str,
    session_id: str,
    language: str,
    chat_history: list,
    metadata: dict,
) -> None:
    state_dict = _load_request_state(request_id)
    if _tts_enabled_for_voice():
        state_dict.update({
            "status": "text_generated",
            "answer_text": answer,
            "language": language,
            "transcribed_text": query,
            "session_id": session_id,
            "metadata": metadata,
        })
        _save_request_state(request_id, state_dict)
        publish_to_queue("audio_tts_queue", {
            "request_id": request_id,
            "answer": answer,
            "user_id": user_id,
            "session_id": session_id,
            "language": language,
            "transcribed_text": query,
        })
        print(f"[>] Voice detected: Routed {request_id} to audio_tts_queue")
    else:
        state_dict.update({
            "status": "success",
            "answer": answer,
            "answer_text": answer,
            "audio_url": None,
            "query": query,
            "transcribed_text": query,
            "language": language,
            "user_id": user_id,
            "session_id": session_id,
            "history": chat_history[-10:],
            "metadata": metadata,
        })
        state_dict.setdefault("metadata", {})
        state_dict["metadata"]["tts_skipped"] = True
        _save_request_state(request_id, state_dict)
        print(f"[✓] Voice task {request_id} completed as text-only (TTS disabled).")


def _process_vector_search_synthesis(payload: dict) -> None:
    request_id = payload.get("request_id")
    query = payload.get("query")
    user_id = payload.get("user_id")
    session_id = payload.get("session_id") or user_id
    language = payload.get("language", "en")
    is_voice = payload.get("is_voice", False)
    context_text = payload.get("context_text") or ""

    print(f"\n[*] [LLM Worker] Synthesizing vector-search answer for: {request_id}")

    state_dict = _load_request_state(request_id)
    # Preserve vector_search_success status; only track that synthesis is in progress
    state_dict["tool"] = "vector_search"
    state_dict["synthesis_phase"] = "synthesizing"
    state_dict["session_id"] = session_id
    _save_request_state(request_id, state_dict)

    chat_history = _get_history_from_cache(user_id, session_id=session_id)
    vector_search_bundle = {
        "success": True,
        "context_text": context_text,
        "routed_doctypes": (state_dict.get("vector_search") or {}).get("routed_doctypes") or [],
        "results_count": (state_dict.get("vector_search") or {}).get("results_count") or 0,
        "search_time": (state_dict.get("vector_search") or {}).get("search_time"),
        "user_context": "personalized" if state_dict.get("user_id") else "general",
        "metadata": (state_dict.get("vector_search") or {}).get("metadata") or {},
    }

    out = synthesize_vector_search_answer(
        query=query,
        vector_search_bundle=vector_search_bundle,
        chat_history=chat_history,
    )
    answer = out.get("answer", "")
    metadata = out.get("metadata", {}) or {}

    new_messages = [
        {"role": "user", "content": query},
        {"role": "assistant", "content": answer},
    ]
    chat_history.extend(new_messages)
    _save_history_to_cache(user_id, new_messages, session_id=session_id)
    _append_history_to_db(
        user_id,
        new_messages,
        session_id=session_id,
        metadata={"source": "llm_worker", "stage": "vector_search_synthesis"},
    )

    if is_voice:
        _finalize_voice_answer(
            request_id=request_id,
            answer=answer,
            query=query,
            user_id=user_id,
            session_id=session_id,
            language=language,
            chat_history=chat_history,
            metadata=metadata,
        )
    else:
        state_dict = _load_request_state(request_id)
        # Merge worker-side timings (refine_query_ms, route_ms) into final metadata
        state_timings = (state_dict.get("metadata") or {}).get("timings_ms") or {}
        metadata.setdefault("timings_ms", {})
        metadata["timings_ms"] = {**state_timings, **metadata["timings_ms"]}
        state_dict.update({
            "status": "success",
            "answer": answer,
            "query": query,
            "user_id": user_id,
            "session_id": session_id,
            "history": chat_history[-10:],
            "metadata": metadata,
        })
        _save_request_state(request_id, state_dict)
        print(f"[✓] Vector search synthesis completed for {request_id}.")

def process_message(ch, method, properties, body):
    """Callback triggered when a message is pulled from text_query_queue."""
    payload = json.loads(body)
    request_id = payload.get("request_id")
    query = payload.get("query")
    user_id = payload.get("user_id")
    
    # Flags passed by the STT worker for voice queries
    is_voice = payload.get("is_voice", False)
    language = payload.get("language", "en")
    session_id = payload.get("session_id") or user_id

    if payload.get("stage") == "vector_search_synthesis":
        try:
            _process_vector_search_synthesis(payload)
        except Exception as e:
            print(f"[x] Vector search synthesis failed for {request_id}: {str(e)}")
            frappe.log_error(f"Vector search synthesis error: {str(e)}", "RabbitMQ Worker")
            frappe.cache().set(request_id, json.dumps({
                "status": "failed",
                "error": str(e),
                "query": query,
                "user_id": user_id,
                "session_id": session_id,
            }))

        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"\n[*] [LLM Worker] Picked up task: {request_id} | Query: '{query}' | Session: {session_id}")

    try:
        # 1. Mark as in-progress for real-time UI feedback
        state_dict = _load_request_state(request_id)
        state_dict["status"] = "generating_answer"
        state_dict["session_id"] = session_id
        state_dict.setdefault("metadata", {})
        state_dict["metadata"].setdefault("timings_ms", {})
        _save_request_state(request_id, state_dict)

        # 2. Load history FIRST — needed for query refinement
        chat_history = _get_history_from_cache(user_id, session_id=session_id)

        # 3. Refine query with conversation context
        refined_query = query
        refine_start = time.perf_counter()
        try:
            result = _refine_query_with_history(query, chat_history) or query
            if isinstance(result, str):
                refined_query = result
        except Exception:
            pass
        refine_ms = int((time.perf_counter() - refine_start) * 1000)

        # 4. Route on refined query (not raw query)
        route_start = time.perf_counter()
        primary_tool = choose_tool(refined_query)
        route_ms = int((time.perf_counter() - route_start) * 1000)

        # KB guard: content/navigation queries don't belong in knowledge_bank
        _KB_CONTENT_WORDS = ("activity", "video", "topic", "course", "explain", "what is", "lesson", "quiz", "assignment")
        if primary_tool == "knowledge_bank" and any(w in refined_query.lower() for w in _KB_CONTENT_WORDS):
            print(f"> KB guard triggered — rerouting '{refined_query[:60]}' to vector_search")
            primary_tool = "vector_search"

        # 5. Update state with routing decision and timings
        state_dict = _load_request_state(request_id)
        state_dict["tool"] = primary_tool
        state_dict["router_timing_ms"] = route_ms
        state_dict["router_decision"] = {"tool": primary_tool, "status": "success"}
        state_dict["metadata"]["timings_ms"].update({
            "refine_query_ms": refine_ms,
            "route_ms": route_ms,
        })
        if refined_query != query:
            state_dict["metadata"]["refined_query"] = refined_query
        _save_request_state(request_id, state_dict)

        # 6. Execute vector search (retrieval + async synthesis)
        if primary_tool == "vector_search":
            vector_search_bundle = retrieve_vector_search(
                query=query,
                refined_query=refined_query,
                chat_history=chat_history,
            )

            if not vector_search_bundle.get("success"):
                state_dict.update({
                    "status": "vector_search_failed",
                    "tool": "vector_search",
                    "error": vector_search_bundle.get("error") or "Vector search failed.",
                    "vector_search": {
                        "status": "failed",
                        "raw_status": "vector_search_failed",
                        "results_count": vector_search_bundle.get("results_count") or 0,
                        "routed_doctypes": vector_search_bundle.get("routed_doctypes") or [],
                        "search_time": vector_search_bundle.get("search_time"),
                        "metadata": vector_search_bundle.get("metadata") or {},
                    },
                    "metadata": vector_search_bundle.get("metadata") or {},
                })
                _save_request_state(request_id, state_dict)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            state_dict.update({
                "status": "vector_search_success",
                "tool": "vector_search",
                "router_decision": {
                    "tool": "vector_search",
                    "status": "success",
                },
                "vector_search": {
                    "status": "success",
                    "raw_status": "vector_search_success",
                    "results_count": vector_search_bundle.get("results_count") or 0,
                    "routed_doctypes": vector_search_bundle.get("routed_doctypes") or [],
                    "search_time": vector_search_bundle.get("search_time"),
                    "metadata": vector_search_bundle.get("metadata") or {},
                    "phase_complete": True,
                },
                "metadata": vector_search_bundle.get("metadata") or {},
            })
            _save_request_state(request_id, state_dict)

            _publish_vector_search_synthesis(
                request_id=request_id,
                query=query,
                user_id=user_id,
                session_id=session_id,
                context_text=vector_search_bundle.get("context_text") or "",
                is_voice=is_voice,
            )
            print(f"[>] Vector search completed for {request_id}; synthesis queued.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 7. Run the existing router logic for SQL/direct flows
        out = process_query(
            query=query,
            chat_history=chat_history,
            voice_mode=is_voice,
            primary_tool=primary_tool,
            refined_query=refined_query,
        )
        answer = out.get("answer", "")
        resolved_tool = _resolve_result_tool(out, primary_tool)

        # 5. Update and save history
        new_messages = [
            {"role": "user", "content": query},
            {"role": "assistant", "content": answer},
        ]
        chat_history.extend(new_messages)
        _save_history_to_cache(user_id, new_messages, session_id=session_id)
        _append_history_to_db(
            user_id,
            new_messages,
            session_id=session_id,
            metadata={"source": "llm_worker"},
        )

        # 6. Routing Logic (Voice vs Text)
        if is_voice:
            metadata = out.get("metadata", {}) or {}
            if state_dict.get("metadata"):
                existing_metadata = state_dict.get("metadata") or {}
                existing_timings = existing_metadata.get("timings_ms") or {}
                out_timings = metadata.get("timings_ms") or {}
                metadata["timings_ms"] = {**existing_timings, **out_timings}

            if _tts_enabled_for_voice():
                # Update state so the frontend knows text is done, audio is next.
                metadata = _apply_end_to_end_timing(state_dict, metadata)
                state_dict.update({
                    "status": "text_generated",
                    "answer_text": answer,
                    "language": language,
                    "transcribed_text": query,
                    "session_id": session_id,
                    "metadata": metadata,
                    "tool": resolved_tool,
                    "router_decision": {
                        "tool": resolved_tool,
                        "status": "success",
                    },
                    "timing_ms": metadata.get("timings_ms", {}).get("total"),
                })
                frappe.cache().set(request_id, json.dumps(state_dict))

                # Publish to TTS queue for the final voice step.
                publish_to_queue("audio_tts_queue", {
                    "request_id": request_id,
                    "answer": answer,
                    "user_id": user_id,
                    "session_id": session_id,
                    "language": language,
                    "transcribed_text": query
                })
                print(f"[>] Voice detected: Routed {request_id} to audio_tts_queue")
            else:
                # Text-only fast path for voice mode: finalize immediately.
                metadata = _apply_end_to_end_timing(state_dict, metadata)
                state_dict.update({
                    "status": "success",
                    "answer": answer,
                    "answer_text": answer,
                    "audio_url": None,
                    "query": query,
                    "transcribed_text": query,
                    "language": language,
                    "user_id": user_id,
                    "session_id": session_id,
                    "history": chat_history[-10:],
                    "metadata": metadata,
                    "tool": resolved_tool,
                    "router_decision": {
                        "tool": resolved_tool,
                        "status": "success",
                    },
                    "timing_ms": metadata.get("timings_ms", {}).get("total"),
                })
                state_dict.setdefault("metadata", {})
                state_dict["metadata"]["tts_skipped"] = True
                frappe.cache().set(request_id, json.dumps(state_dict))
                print(f"[✓] Voice task {request_id} completed as text-only (TTS disabled).")

        else:
            # Standard Text Query - Finish and save to Redis
            metadata = _apply_end_to_end_timing(state_dict, out.get("metadata", {}) or {})

            state_dict.update({
                "status": "success",
                "answer": answer,
                "query": query,
                "user_id": user_id,
                "session_id": session_id,
                "history": chat_history[-10:],
                "metadata": metadata,
                "tool": resolved_tool,
                "router_decision": {
                    "tool": resolved_tool,
                    "status": "success",
                },
                "timing_ms": metadata.get("timings_ms", {}).get("total"),
            })
            frappe.cache().set(request_id, json.dumps(state_dict))
            print(f"[✓] Task {request_id} completed successfully.")

    except Exception as e:
        print(f"[x] Task {request_id} failed: {str(e)}")
        frappe.log_error(f"LLM Worker Error: {str(e)}", "RabbitMQ Worker")
        
        # Save failure state to Redis
        frappe.cache().set(request_id, json.dumps({
            "status": "failed",
            "error": str(e),
            "query": query,
            "user_id": user_id
        }))

    # 6. Acknowledge the message (Removes it from RabbitMQ)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start():
    """Initializes RabbitMQ connection and starts consuming."""
    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
    
    try:
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.queue_declare(queue="text_query_queue", durable=True)
        channel.basic_qos(prefetch_count=1) # Process one message at a time
        channel.basic_consume(queue="text_query_queue", on_message_callback=process_message)

        print(" [*] LLM Worker running. Waiting for messages. (CTRL+C to exit)")
        channel.start_consuming()

    except Exception as e:
        print(f"[!] Worker crashed: {str(e)}")
# tap_ai/workers/llm_worker.py

import frappe
import json
import pika
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
    frappe.cache().set(request_id, json.dumps(state_dict))


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

    chat_history.append({"role": "user", "content": query})
    chat_history.append({"role": "assistant", "content": answer})
    _save_history_to_cache(user_id, chat_history, session_id=session_id)
    _append_history_to_db(
        user_id,
        [{"role": "user", "content": query}, {"role": "assistant", "content": answer}],
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

    primary_tool = choose_tool(query)

    print(f"\n[*] [LLM Worker] Picked up task: {request_id} | Query: '{query}' | Session: {session_id}")

    try:
        # 1. Update status to provide real-time UI feedback
        state_dict = _load_request_state(request_id)
        state_dict["status"] = "generating_answer"
        state_dict["session_id"] = session_id
        _save_request_state(request_id, state_dict)

        # 2. Fetch history using your existing router helper
        chat_history = _get_history_from_cache(user_id, session_id=session_id)

        # 3. Split vector search into retrieval and synthesis when routed there
        if primary_tool == "vector_search":
            vector_search_bundle = retrieve_vector_search(
                query=query,
                chat_history=chat_history,
            )

            if not vector_search_bundle.get("success"):
                state_dict.update({
                    "status": "vector_search_failed",
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

        # 4. Run the existing router logic for SQL/direct flows
        out = process_query(query=query, chat_history=chat_history, voice_mode=is_voice)
        answer = out.get("answer", "")

        # 5. Update and save history
        chat_history.append({"role": "user", "content": query})
        chat_history.append({"role": "assistant", "content": answer})
        _save_history_to_cache(user_id, chat_history, session_id=session_id)
        _append_history_to_db(
            user_id,
            [{"role": "user", "content": query}, {"role": "assistant", "content": answer}],
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
                state_dict.update({
                    "status": "text_generated",
                    "answer_text": answer,
                    "language": language,
                    "transcribed_text": query,
                    "session_id": session_id,
                    "metadata": metadata,
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
                frappe.cache().set(request_id, json.dumps(state_dict))
                print(f"[✓] Voice task {request_id} completed as text-only (TTS disabled).")

        else:
            # Standard Text Query - Finish and save to Redis
            metadata = out.get("metadata", {})

            state_dict.update({
                "status": "success",
                "answer": answer,
                "query": query,
                "user_id": user_id,
                "session_id": session_id,
                "history": chat_history[-10:],
                "metadata": metadata,
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
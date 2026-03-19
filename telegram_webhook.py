# tap_ai/telegram_webhook.py
# Telegram Channel Adapter for Frappe AI Backend

import os
import time
import uuid
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from openai import OpenAI

# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
FRAPPE_API_URL = os.getenv("FRAPPE_API_URL")
FRAPPE_API_RESULT_URL = os.getenv("FRAPPE_API_RESULT_URL")
FRAPPE_API_KEY = os.getenv("FRAPPE_API_KEY")
FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

app = Flask(__name__)

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

HEADERS = {
    "Authorization": f"token {FRAPPE_API_KEY}:{FRAPPE_API_SECRET}",
    "Content-Type": "application/json"
}

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def telegram_get_file(file_id: str) -> str:
    """Get downloadable Telegram file URL."""
    r = requests.get(f"{TELEGRAM_API_BASE}/getFile", params={"file_id": file_id})
    r.raise_for_status()
    file_path = r.json()["result"]["file_path"]
    return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"


def whisper_transcribe(file_url: str) -> tuple[str, str]:  
    """Download audio and transcribe using Whisper."""  
    audio = requests.get(file_url).content  
  
    # Generate unique temp filename to avoid race conditions  
    temp_filename = f"/tmp/input_{uuid.uuid4().hex}.ogg"  
      
    try:  
        with open(temp_filename, "wb") as f:  
            f.write(audio)  
  
        with open(temp_filename, "rb") as audio_file:  
            transcript = client.audio.transcriptions.create(  
                model="gpt-4o-transcribe",  
                file=audio_file  
            )  
    finally:  
        # Cleanup temp file  
        if os.path.exists(temp_filename):  
            os.remove(temp_filename)  
  
    text = transcript.text  
    language = getattr(transcript, "language", "unknown")  
  
    return text, language


def call_query_api(text: str, user_id: str) -> str:
    params = {
        "q": text,
        "user_id": user_id
    }

    r = requests.post(
        FRAPPE_API_URL,
        params=params,
        headers=HEADERS,
        timeout=30
    )
    r.raise_for_status()

    return r.json()["message"]["request_id"]


def poll_result(request_id: str, timeout_sec: int = 60) -> dict:
    start = time.time()

    while time.time() - start < timeout_sec:
        r = requests.get(
            FRAPPE_API_RESULT_URL,
            params={"request_id": request_id},
            headers=HEADERS,
            timeout=15
        )
        r.raise_for_status()

        data = r.json()["message"]

        if data["status"] in ("success", "failed"):
            return data

        time.sleep(2)

    return {
        "status": "failed",
        "answer": "Request timed out."
    }



def tts_generate(text: str) -> str:  
    """Generate MP3 audio from text using OpenAI TTS."""  
    # Generate unique temp filename  
    temp_filename = f"/tmp/output_{uuid.uuid4().hex}.mp3"  
  
    try:  
        with client.audio.speech.with_streaming_response.create(  
            model="gpt-4o-mini-tts",  
            voice="alloy",  
            input=text  
        ) as response:  
            response.stream_to_file(temp_filename)  
          
        return temp_filename  
    except Exception:  
        # Cleanup on error  
        if os.path.exists(temp_filename):  
            os.remove(temp_filename)  
        raise


def send_text(chat_id: int, text: str):
    payload = {"chat_id": chat_id, "text": text}
    requests.post(f"{TELEGRAM_API_BASE}/sendMessage", json=payload, timeout=20)


def send_voice(chat_id: int, audio_path: str):
    with open(audio_path, "rb") as audio:
        files = {"voice": audio}
        data = {"chat_id": chat_id}
        requests.post(
            f"{TELEGRAM_API_BASE}/sendVoice",
            data=data,
            files=files,
            timeout=30
        )

# -------------------------------------------------------------------
# Webhook
# -------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json()
    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")

    if not chat_id:
        return jsonify(success=True)

    user_id = f"telegram:{chat_id}"
    is_voice_input = False

    try:
        # ---------------------------
        # TEXT MESSAGE
        # ---------------------------
        if "text" in message:
            user_text = message["text"]

        # ---------------------------
        # VOICE MESSAGE
        # ---------------------------
        elif "voice" in message:
            is_voice_input = True
            file_id = message["voice"]["file_id"]
            file_url = telegram_get_file(file_id)
            user_text, language = whisper_transcribe(file_url)

        else:
            send_text(chat_id, "Unsupported message type.")
            return jsonify(success=True)

        # ---------------------------
        # Call Frappe
        # ---------------------------
        request_id = call_query_api(user_text, user_id)
        result = poll_result(request_id)

        if result["status"] == "success":
            answer_text = result["answer"]

            # Voice input → voice output
            if is_voice_input:
                try:
                    audio_path = tts_generate(answer_text)
                    send_voice(chat_id, audio_path)
                except Exception as tts_err:
                    print("TTS failed:", tts_err)
                    send_text(chat_id, answer_text)
            else:
                send_text(chat_id, answer_text)

        else:
            send_text(chat_id, "Sorry, something went wrong while processing your request.")

    except Exception as e:
        print("Webhook error:", e)
        send_text(chat_id, "Internal error. Please try again later.")

    return jsonify(success=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

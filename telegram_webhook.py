# tap_ai/telegram_webhook.py
# Flask bridge between Telegram Bot and Frappe API

import os
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
FRAPPE_API_URL = os.getenv("FRAPPE_API_URL")
FRAPPE_API_KEY = os.getenv("FRAPPE_API_KEY")
FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET")
HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")

# --- Validate critical variables ---
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("FATAL: TELEGRAM_BOT_TOKEN is missing in .env")

if not FRAPPE_API_URL:
    raise ValueError("FATAL: FRAPPE_API_URL is missing in .env")

print(f"Loaded Telegram Bot Token: {TELEGRAM_BOT_TOKEN[:10]}... (hidden for security)")

# Flask app
app = Flask(__name__)

# Proxy dictionary for requests
PROXIES = {}
if HTTP_PROXY:
    PROXIES['http'] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES['https'] = HTTPS_PROXY

# Telegram send message URL
TELEGRAM_SEND_MESSAGE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    """
    Handles incoming Telegram messages.
    """
    update = request.get_json()

    if "message" in update and "text" in update["message"]:
        chat_id = update["message"]["chat"]["id"]
        user_query = update["message"]["text"]

        print(f"Received message from chat_id {chat_id}: '{user_query}'")

        if user_query == "/start":
            send_telegram_message(chat_id,
                "Hi, I'm your educational Assistant! Ask me anything related to your course, projects, or assignments.")
            return jsonify(success=True)

        user_id = f"telegram:{chat_id}"

        try:
            headers = {
                'Authorization': f'token {FRAPPE_API_KEY}:{FRAPPE_API_SECRET}',
                'Content-Type': 'application/json'
            }
            payload = {
                'q': user_query,
                'user_id': user_id
            }

            response = requests.post(FRAPPE_API_URL, json=payload, headers=headers, timeout=60, proxies=PROXIES or None)
            response.raise_for_status()
            api_result = response.json()

            if 'message' in api_result and 'answer' in api_result['message']:
                answer_text = api_result['message']['answer']
            else:
                answer_text = str(api_result)

        except requests.exceptions.RequestException as e:
            print(f"Error calling Frappe API: {e}")
            answer_text = "Sorry, I'm having trouble connecting to my brain right now. Please try again later."
        except Exception as e:
            print(f"Unexpected error: {e}")
            answer_text = "An unexpected error occurred. Please check the logs."

        send_telegram_message(chat_id, answer_text)

    return jsonify(success=True)

import re
def clean_markdown(text: str) -> str:
    """
    Cleans text for Telegram Markdown while preserving proper formatting:
    - Keeps valid bold, italic, and links intact.
    - Escapes stray markdown characters only.
    - Prevents Telegram '400 Bad Request' errors.
    """
    # Escape only characters not part of a Markdown link or formatting
    # Avoid escaping: **bold**, _italic_, [text](url)
    
    # First, handle backslashes that are unnecessary
    text = text.replace("\\", "")

    # Escape stray special chars not inside proper markdown syntax
    # For example: a single * or _ that isn't wrapped properly
    text = re.sub(r'(?<!\*)\*(?!\*)', r'\*', text)  # leave **bold** untouched
    text = re.sub(r'(?<!_)_(?!_)', r'\_', text)      # leave _italic_ untouched

    return text


def send_telegram_message(chat_id, text):
    """
    Sends a text message to a given chat_id via the Telegram Bot API.
    Handles markdown errors and message length limits.
    """
    max_len = 4000  # Telegram safe limit
    chunks = [text[i:i+max_len] for i in range(0, len(text), max_len)]

    for chunk in chunks:
        payload = {
            'chat_id': chat_id,
            'text': clean_markdown(chunk),
            'parse_mode': 'Markdown'
        }
        try:
            response = requests.post(TELEGRAM_SEND_MESSAGE_URL, json=payload, timeout=30, proxies=PROXIES or None)
            if response.status_code == 400:
                print("Markdown error detected, retrying with plain text")
                payload.pop('parse_mode')
                payload['text'] = chunk
                retry_response = requests.post(TELEGRAM_SEND_MESSAGE_URL, json=payload, timeout=30, proxies=PROXIES or None)
                retry_response.raise_for_status()
            else:
                response.raise_for_status()
            print(f"Successfully sent response to chat_id {chat_id}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending message to Telegram: {e}")


if __name__ == '__main__':
    # Run without debug auto-reload to prevent botNone issues
    app.run(host="0.0.0.0", port=5000, debug=False)

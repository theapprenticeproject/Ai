# TAP AI - Conversational AI Engine

This project extends the TAP AI Frappe application with a powerful, conversational AI layer. It provides a single, robust API endpoint that can understand user questions and intelligently route them to the best tool—either a direct database query or a semantic vector search—to provide accurate, context-aware answers.

The system is designed for multi-turn conversations, automatically managing chat history to understand follow-up questions.

## 🚀 Core Architecture

The system's intelligence lies in its central router, which acts as a decision-making brain. When a query is received, it follows this flow:

1. **Intelligent Routing:** An LLM analyzes the user's query to determine its intent.
2. **Tool Selection:**
   - For factual, specific questions (e.g., "list all...", "how many..."), it selects the **Text-to-SQL Engine**.
   - For conceptual, open-ended, or summarization questions (e.g., "summarize...", "explain..."), it selects the **Vector RAG Engine**.
3. **Execution & Fallback:** The chosen engine executes the query. If it fails to produce a satisfactory answer, the system automatically falls back to the Vector RAG engine as a safety net.
4. **Answer Synthesis:** The retrieved data is passed to an LLM, which generates a final, human-readable answer.

```mermaid
graph TD
    subgraph "User Input"
        User[User Query]
    end

    subgraph "Orchestration Layer"
        Router["services/router.py<br><b>Intelligent Router</b>"]
    end

    subgraph "Decision Engine"
        ChooseTool{LLM: Choose Tool}
    end

    subgraph "Execution Engines"
        SQL["services/sql_answerer.py<br><b>Text-to-SQL Engine</b>"]
        RAG["services/rag_answerer.py<br><b>Vector RAG Engine</b>"]
    end

    subgraph "Data Sources & Dependencies"
        MariaDB[(Frappe MariaDB)]
        PineconeDB[(Pinecone Vector DB)]
        DocSelector["services/doctype_selector.py"]
    end

    subgraph "Final Output"
        Laiynthesis["LLM: Synthesize Final Answer"]
        FinalAnswer[User-Facing Answer]
    end

    %% Main Flow
    User --> Router
    Router --> ChooseTool

    %% Tool Selection Paths
    ChooseTool -- "Factual/Specific Query" --> SQL
    ChooseTool -- "Conceptual/Vague Query" --> RAG

    %% Engine Dependencies
    SQL -- "Generates & Runs SQL" --> MariaDB
    RAG -- "Selects Relevant DocTypes" --> DocSelector
    RAG -- "Performs Vector Search" --> PineconeDB

    %% Fallback Logic
    SQL -- "Fallback on Failure" --> RAG

    %% Answer Synthesis
    SQL -- "Returns Raw Data" --> Laiynthesis
    RAG -- "Returns Rich Context" --> Laiynthesis
    Laiynthesis --> FinalAnswer
```
## ⚙️ Engine Robustness

The robustness of the system comes from the specialized design of each engine. The following diagrams illustrate their internal workflows.

### Text-to-SQL Engine: From Query to Structured Data

This engine excels at factual queries because it builds an "intelligent schema" before prompting the LLM, ensuring the generated SQL is highly accurate.

```mermaid
graph TD
    subgraph "Input"
        A[User Query]
    end

    subgraph "Intelligent Schema Builder (sql_answerer.py)"
        B["1. Inspect Live Frappe Metadata"]
        B1["- Identify 'Select' fields & their Options (e.g., 'Basic', 'Intermediate')"]
        B2["- Identify 'Link' & 'Table' fields to understand relationships"]
        B3["- Read `allowed_joins` from schema.json"]
        B --> B1 & B2 & B3
        C["2. Create Rich Schema Prompt for LLM"]
        B1 & B2 & B3 --> C
    end

    subgraph "SQL Generation"
        D{LLM: Generate SQL}
        C --> D
    end

    subgraph "Execution"
        E[MariaDB]
        D -- "SELECT ... WHERE ... JOIN ..." --> E
    end
    
    subgraph "Output"
        F[Structured Data Rows]
        E --> F
    end

    A --> B
```

### Vector RAG Engine: From Query to Rich Context

This engine excels at conceptual and conversational queries by refining the user's intent and retrieving semantically relevant, unstructured text.

```mermaid
graph TD
    subgraph "Input"
        A[User Query + Chat History]
    end

    subgraph "Conversational Refiner (rag_answerer.py)"
        B{LLM: Refine Query with History}
        C["Creates a standalone query<br><i>e.g., 'summarize the first one' -> 'summarize Video XYZ'</i>"]
        B --> C
    end

    subgraph "Retrieval Pipeline"
        D["1. Select DocTypes<br>(doctype_selector.py)"]
        E["2. Semantic Search<br>(pinecone_store.py)"]
        F["3. Two-Step Fetch<br>Get full text from MariaDB"]
        D --> E --> F
    end

    subgraph "Dependencies"
        Pinecone[(Pinecone Vector DB)]
        MariaDB[(Frappe MariaDB)]
        E --> Pinecone
        F --> MariaDB
    end

    subgraph "Output"
        G[Rich Context Text Chunks]
        F --> G
    end

    A --> B
    C --> D
```


## 📦 Installation

Ensure TAP AI is installed on the site:

```bash
bench get-app tap_ai <repo-url>
bench --site <site-name> install-app tap_ai
```

Add the code for this AI engine to the tap_ai app directory.

Finally, install the required Python libraries into the bench's virtual environment:

```bash
bench pip install langchain-openai pinecone-client frappe-client
```

## ⚙️ Configuration

Add the following keys to the site's `site_config.json`:

```json
{
  "openai_api_key": "sk-xxxx",
  "primary_llm_model": "gpt-4o-mini",
  "embedding_model": "text-embedding-3-small",

  "pinecone_api_key": "pcn-xxxx",
  "pinecone_index": "tap-ai-byo"
}
```

## 🧭 One-Time Setup

Follow these steps in order to initialize the system.

### 1) Generate the Database Schema

This script inspects the Frappe DocTypes and creates a `tap_ai_schema.json` file. This schema is crucial for both the Text-to-SQL and Vector RAG engines.

```bash
# Run this from app's root directory
python3 -m tap_ai.schema.generate_schema
```

### 2) Create the Pinecone Index

This command prepares Pinecone account by creating the vector index where document embeddings will be stored.

```bash
bench execute tap_ai.services.pinecone_index.cli_ensure_index
```

### 3) Populate the Pinecone Index (Upsert Data)

This command reads schema, processes the data from allow-listed DocTypes, creates embeddings, and saves them to Pinecone.

```bash
bench execute tap_ai.services.pinecone_store.cli_upsert_all
```

## 🧪 Testing from the Command Line

The primary entry point for all testing is the main router's CLI. It automatically manages conversation history for a given `user_id`.

### Turn 1: Ask an initial question

This can be a factual query (handled by SQL) or a conceptual one (handled by RAG).

```bash
# Factual Query (will use Text-to-SQL)
bench execute tap_ai.services.router.cli --kwargs "{'q':'list all course videos with basic difficulty', 'user_id':'test_user_1'}"

# Conceptual Query (will use Vector RAG)
bench execute tap_ai.services.router.cli --kwargs "{'q':'summarize the finlit video on needs vs wants', 'user_id':'test_user_1'}"
```

### Turn 2: Ask a follow-up question

The system will automatically use the cache to retrieve the history for `test_user_1` and understand the context.

```bash
bench execute tap_ai.services.router.cli --kwargs "{'q':'summarize the first one', 'user_id':'test_user_1'}"
```

## 🌐 Production REST API

### Endpoint

```
POST /api/method/tap_ai.api.query.query
```

### Authentication

The API uses Frappe's standard token-based authentication. Use an API Key and Secret generated from a dedicated API user.

```
Authorization: token <api_key>:<api_secret>
```

### Request Body

The API accepts a JSON body. The `user_id` is critical for maintaining separate conversation histories for different users (e.g., different WhatsApp numbers).

```json
{
    "q": "Your question here",
    "user_id": "whatsapp:+911234567890"
}
```

### Example curl Command

This is how an external service (like GCP webhook for WhatsApp) would call the API.

```bash
curl -X POST "http://your.frappe.site/api/method/tap_ai.api.query.query" \
 -H "Authorization: token <your_api_key>:<your_api_secret>" \
 -H "Content-Type: application/json" \
 -d '{
    "q": "summarize the video about goal setting",
    "user_id": "whatsapp:+911234567890"
 }'
```

## 🔍 Core File Descriptions

**`tap_ai/api/query.py`**: The production-ready REST API endpoint. It handles requests, manages authentication and rate limiting, and orchestrates the conversational flow by managing user-specific chat history in the cache.

**`tap_ai/services/router.py`**: The central brain of the system. It takes a user's query and chat history, uses an LLM to choose the best tool (`text_to_sql` or `vector_search`), and manages the fallback logic.

**`tap_ai/services/sql_answerer.py`**: The Text-to-SQL engine. It uses an intelligent schema builder to give an LLM rich context about the database, enabling it to generate accurate SQL queries for factual questions.

**`tap_ai/services/rag_answerer.py`**: The Vector RAG engine. It handles conceptual and summarization questions by finding semantically similar documents in the Pinecone index and using them as context for an LLM to synthesize an answer.

**`tap_ai/services/pinecone_store.py`**: Manages all interactions with the Pinecone vector database, including the data upsert pipeline and the search logic.

**`tap_ai/services/pinecone_index.py`**: Manages the Pinecone index lifecycle, including creation and deletion, via command-line functions.

**`tap_ai/services/doctype_selector.py`**: A crucial pre-processing step that uses an LLM to intelligently select the most relevant DocTypes for a given query, narrowing the search space for the RAG engine.

**`tap_ai/services/ratelimit.py`**: A utility for enforcing rate limits on API usage, using the Frappe cache to track requests.

**`tap_ai/schema/generate_schema.py`**: A utility script to generate a detailed JSON representation of your Frappe DocTypes, which is used by both the SQL and RAG engines.

**`tap_ai/infra/config.py`**: A centralized helper for retrieving configuration settings (like API keys) from `site_config.json`.

**`tap_ai/infra/sql_catalog.py`**: A simple loader for the `tap_ai_schema.json` file, making it accessible across different services.

## 🤖 Telegram Bot Demo (Local Setup)

This guide explains how to connect the AI engine to a Telegram bot for a live, local demonstration.

### Architecture Overview

```mermaid
graph LR
    User -- "Sends Message" --> Telegram
    Telegram -- "Webhook POST" --> Ngrok
    Ngrok -- "Forwards to" --> LocalBridge["telegram_webhook.py<br>(Local Python Script)"]
    LocalBridge -- "Calls API" --> FrappeAPI["Frappe API<br>(localhost:8000)"]
    FrappeAPI -- "Gets Answer" --> Router["AI Engine"]
    Router -- "Returns Answer" --> FrappeAPI
    FrappeAPI -- "Sends Answer" --> LocalBridge
    LocalBridge -- "Replies to User" --> Telegram
```

### Step 1: Create a Telegram Bot

1. Open Telegram and search for `@BotFather`.
2. Start a chat and send the command `/newbot`.
3. Follow the instructions to give the bot a name and username.
4. **BotFather will provide a unique access token** that looks like `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`. Copy this token securely.

### Step 2: Expose Local Server with Ngrok

Ngrok creates a secure, public URL that tunnels to a port on the local machine. This enables Telegram servers to communicate with the local development environment.

1. **Install Ngrok:** Follow instructions at ngrok.com.
2. **Authenticate Ngrok:** Run `ngrok config add-authtoken <token>` once.
3. **Start Ngrok:** The Python bridge script runs on port `5000`. In a new terminal window, navigate to the `frappe-bench` directory and run:

```bash
ngrok http 5000
```

4. Ngrok will display a **Forwarding URL** like `https://random-string-123.ngrok-free.app`. Copy this HTTPS URL.

### Step 3: Set Up and Run the Telegram Bridge

The `telegram_webhook.py` script acts as a bridge between Telegram and the Frappe API. It's a web server that listens for messages from Ngrok.

1. **Install Libraries:**

```bash
bench pip install python-telegram-bot Flask requests
```

2. **Configure Credentials:** Open `telegram_webhook.py` and replace the placeholder values with actual credentials.
3. **Run the Script:** In another terminal window, navigate to `frappe-bench`, activate the virtual environment, and run:

```bash
# Activate the environment
source env/bin/activate
# Run the bridge
python apps/tap_ai/telegram_webhook.py
```

The output should show `* Running on http://127.0.0.1:5000`.

### Step 4: Set the Telegram Webhook

Configure Telegram to send all bot messages to the public Ngrok URL.

1. **Construct the URL:** Combine the Bot Token and Ngrok HTTPS URL.
2. **Run the Command:** In a fourth terminal window, execute the curl command with actual values:

```bash
curl -F "url=https://<NGROK_URL>/webhook" \
     "https://api.telegram.org/bot<BOT_TOKEN>/setWebhook"
```

**Example:**

```bash
curl -F "url=https://random-string-123.ngrok-free.app/webhook" \
     "https://api.telegram.org/bot123456:ABC-DEF1234/setWebhook"
```

3. The response should be `{"ok":true,"result":true,"description":"Webhook was set"}`.

### Step 5: Test the Bot

With all components running:
- Frappe bench is active
- Ngrok tunnel is established
- Python bridge script is listening

Open Telegram, find the bot, and start a conversation. 

<img width="669" height="926" alt="Screenshot 2025-09-09 194755" src="https://github.com/user-attachments/assets/68fc5401-192b-4c2e-98cd-71a9d123a5d8" />
<img width="641" height="781" alt="Screenshot 2025-09-09 195216" src="https://github.com/user-attachments/assets/649b6dcd-8bd1-4cb4-a607-7d5f0b81e0e6" />

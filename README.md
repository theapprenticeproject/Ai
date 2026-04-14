# TAP AI - Conversational AI Engine

This project extends the TAP AI Frappe application with a powerful, conversational AI layer. It provides a single, robust API endpoint that can understand user questions and intelligently route them to the best tool—either a direct database query or a semantic vector search—to provide accurate, context-aware answers.

The system is designed for multi-turn conversations, automatically managing chat history to understand follow-up questions. It features **asynchronous processing via RabbitMQ workers**, **voice input/output support**, and **dynamic configuration management** for seamless integration with TAP LMS.

Current deployment topology:
- **AI application server:** `ai.evalix.xyz` (hosts TAP AI code and workers)
- **Remote database server:** `data.evalix.xyz` (PostgreSQL)

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Core Architecture](#-core-architecture)
- [System Workflow](#-system-workflow)
- [Complete Codebase Structure](#-complete-codebase-structure)
- [Dependencies](#-dependencies)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [One-Time Setup](#-one-time-setup)
- [Testing](#-testing)
- [API Documentation](#-api-documentation)
- [Worker System](#-worker-system)
- [Core File Descriptions](#-core-file-descriptions)
- [Telegram Bot Demo](#-telegram-bot-demo-local-setup)
- [Deployment Guide](#-deployment-guide)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Project Overview

**TAP AI** is a conversational AI engine built on top of the Frappe framework. It intelligently routes user queries to specialized execution engines:

- **Text-to-SQL Engine**: For factual, database-specific queries
- **Vector RAG Engine**: For conceptual, semantic, and summarization queries
- **RabbitMQ Worker Architecture**: Asynchronous processing for scalability
- **Voice Processing**: STT → LLM → TTS pipeline for voice queries

**Key Features:**
- Intelligent routing using LLMs
- Multi-turn conversation support with history management
- Hybrid query execution (SQL + Vector Search)
- Automatic fallback mechanisms
- Telegram bot integration
- Rate limiting and authentication built-in
- Voice input/output support via Telegram
- Asynchronous processing with RabbitMQ
- Dynamic configuration for TAP LMS integration
- Admin-controlled DocType exclusion system

**Technology Stack:**
- **Backend**: Python 3.10+
- **Framework**: Frappe (ERPNext)
- **LLM**: OpenAI GPT models
- **Vector DB**: Pinecone
- **Database**: Remote PostgreSQL (`data.evalix.xyz`)
- **Message Queue**: RabbitMQ (Pika)
- **Caching**: Redis
- **Web Framework**: Flask (for Telegram webhooks)
- **ORM**: SQLAlchemy

**Language Composition:**
- **Python**: 107,850 bytes (99%)
- **JavaScript**: 564 bytes (1%)

---

## 🚀 Core Architecture

The system's intelligence lies in its central router, which acts as a decision-making brain. When a query is received, it follows this flow:

1. **Intelligent Routing:** An LLM analyzes the user's query to determine its intent.
2. **Tool Selection:**
   - For factual, specific questions (e.g., "list all...", "how many..."), it selects the **Text-to-SQL Engine**.
   - For conceptual, open-ended, or summarization questions (e.g., "summarize...", "explain..."), it selects the **Vector RAG Engine**.
3. **Execution & Fallback:** The chosen engine executes the query. If it fails to produce a satisfactory answer, the system automatically falls back to the Vector RAG engine as a safety net.
4. **Answer Synthesis:** The retrieved data is passed to an LLM, which generates a final, human-readable answer.

### System Flow Diagram

```mermaid
graph TD
    subgraph "User Input"
        User[User Query]
    end

    subgraph "API Layer"
      QueryAPI["api/query.py<br><b>Unified Query API (Text + Voice)</b>"]
    end

    subgraph "Message Queue"
        RabbitMQ["RabbitMQ<br>Message Broker"]
    end

    subgraph "Worker Processes"
        STTWorker["workers/stt_worker.py<br><b>Speech-to-Text</b>"]
        LLMWorker["workers/llm_worker.py<br><b>LLM Router</b>"]
        TTSWorker["workers/tts_worker.py<br><b>Text-to-Speech</b>"]
    end

    subgraph "Services"
        Router["services/router.py<br><b>Intelligent Router</b>"]
        SQL["services/sql_answerer.py<br><b>SQL Engine</b>"]
        RAG["services/rag_answerer.py<br><b>RAG Engine</b>"]
    end

    subgraph "Data Layer"
      PostgresDB[(Remote PostgreSQL<br>data.evalix.xyz)]
        PineconeDB[(Pinecone<br>Vector DB)]
    end

    User -->|Text or Voice| QueryAPI
    QueryAPI -->|Request| RabbitMQ
    
    RabbitMQ -->|audio_stt_queue| STTWorker
    RabbitMQ -->|text_query_queue| LLMWorker
    RabbitMQ -->|audio_tts_queue| TTSWorker
    
    STTWorker -->|Transcribed Text| RabbitMQ
    LLMWorker -->|Route Query| Router
    Router -->|Factual| SQL
    Router -->|Conceptual| RAG
    
    SQL -->|SQL Query| PostgresDB
    RAG -->|Vector Search| PineconeDB
    
    LLMWorker -->|Answer| TTSWorker
    TTSWorker -->|Audio File| PostgresDB
```

### ⚙️ Engine Robustness

The robustness of the system comes from the specialized design of each engine.

#### Text-to-SQL Engine: From Query to Structured Data

This engine excels at factual queries because it builds an "intelligent schema" before prompting the LLM.

```mermaid
graph TD
    A[User Query] --> B["1. Inspect Live Frappe Metadata"]
    B --> C["2. Create Rich Schema Prompt"]
    C --> D{LLM: Generate SQL}
    D --> E[Remote PostgreSQL data.evalix.xyz]
    E --> F[Structured Data Rows]
```

#### Vector RAG Engine: From Query to Rich Context

This engine excels at conceptual queries by retrieving semantically relevant documents.

```mermaid
graph TD
    A[User Query + Chat History] --> B{LLM: Refine Query}
    B --> C["1. Select DocTypes"]
    C --> D["2. Semantic Search"]
    D --> E["3. Fetch Full Text"]
    E --> F[Rich Context Chunks]
```

---

## 📁 Complete Codebase Structure

```
tap_ai/
├── __init__.py                          # Package initialization
├── hooks.py                             # Frappe hooks for app lifecycle
├── modules.txt                          # Module declaration
├── patches.txt                          # Database migration patches
│
├── api/                                 # REST API Endpoints
│   ├── __init__.py
│   ├── query.py                         # Unified query endpoint (text + voice, async via RabbitMQ)
│   ├── result.py                        # Unified result polling endpoint (with optional server-side wait)
│   ├── voice_query.py                   # Backward-compatible wrapper alias for unified query
│   └── voice_result.py                  # Backward-compatible wrapper alias for unified result
│
├── services/                            # Core execution engines
│   ├── __init__.py
│   ├── router.py                        # Intelligent router (brain of system)
│   ├── sql_answerer.py                  # Text-to-SQL engine
│   ├── rag_answerer.py                  # Vector RAG engine
│   ├── doctype_selector.py              # DocType selection for RAG
│   ├── pinecone_store.py                # Pinecone vector database integration
│   ├── pinecone_index.py                # Pinecone index lifecycle
│   └── ratelimit.py                     # API rate limiting utility
│
├── workers/                             # RabbitMQ Background Workers
│   ├── llm_worker.py                    # Main LLM routing worker
│   ├── stt_worker.py                    # Speech-to-Text worker (Whisper)
│   └── tts_worker.py                    # Text-to-Speech worker (OpenAI TTS)
│
├── schema/                              # Database schema generation
│   ├── __init__.py
│   ├── generate_schema.py               # Schema generator script
│   └── tap_ai_schema.json               # Generated schema file
│
├── infra/                               # Infrastructure utilities
│   ├── __init__.py
│   ├── config.py                        # Centralized config loader
│   └── sql_catalog.py                   # Schema catalog loader
│
├── utils/                               # Utility functions
│   ├── __init__.py
│   ├── dynamic_config.py                # Dynamic config for TAP LMS integration
│   ├── remote_db.py                     # Remote PostgreSQL connection helpers
│   └── mq.py                            # RabbitMQ publisher utility
│
├── config/                              # Frappe app configuration
│   └── __init__.py
│
├── public/                              # Static assets
│   └── .gitkeep
│
├── templates/                           # Frappe templates
│   ├── __init__.py
│   └── pages/
│
└── tap_ai/                              # Additional modules (if any)

# Root-level files

├── README.md                            # This file
├── requirements.txt                     # Python dependencies
├── pyproject.toml                       # Project metadata & build config
├── license.txt                          # License information
├── .env                                 # Local environment variables (do not commit secrets)
├── .gitignore                           # Git ignore rules
├── .vscode/                             # VS Code workspace settings
├── .eslintrc                            # ESLint configuration
├── .editorconfig                        # Editor configuration
├── .pre-commit-config.yaml              # Pre-commit hooks
├── __init__.py                          # Root package init
└── telegram_webhook.py                  # Telegram bot bridge script
```

---

## 📦 Dependencies

### Core Dependencies

#### Database & ORM
- `psycopg2-binary` (or equivalent) - PostgreSQL database driver for remote DB access
- `sqlalchemy>=2.0.32` - SQL toolkit and ORM
- `sqlalchemy-utils>=0.41.2` - SQLAlchemy utility functions

#### LLM & RAG
- `openai>=1.40.0` - OpenAI API client (GPT, Whisper, TTS)
- `langchain>=0.3.0` - LLM framework
- `langchain-community>=0.3.0` - LangChain integrations
- `langchain-openai>=0.1.17` - LangChain OpenAI integration
- `tiktoken>=0.7.0` - Token counting for OpenAI

#### Vector Database
- `pinecone` - Pinecone vector database client

#### Message Queue
- `pika` - RabbitMQ client for async processing

#### Data Processing
- `numpy>=1.26.4` - Numerical computing

#### Caching & Storage
- `redis>=5.0.8` - Redis client for caching and rate limiting

#### Configuration & Utilities
- `python-dotenv>=1.0.1` - Environment variable loading
- `pydantic>=2.8.2` - Data validation
- `loguru>=0.7.2` - Enhanced logging
- `tenacity>=9.0.0` - Retry library

#### Telegram Bot Integration
- `Flask` - Web framework for webhooks
- `python-telegram-bot` - Telegram bot library
- `requests` - HTTP client library

> Note: Telegram integration dependencies are only required for `telegram_webhook.py` and are not included in `requirements.txt` by default.

#### Testing
- `pytest>=8.3.2` - Testing framework
- `httpx>=0.27.2` - Async HTTP client

### Framework
- `Frappe~=15.0+` - Installed via bench (not in requirements.txt)

---

## 📦 Installation

### Prerequisites

- Python 3.10+
- Frappe bench installed
- Remote PostgreSQL server reachable (`data.evalix.xyz`)
- RabbitMQ broker running
- Redis server running
- Pinecone account (for Vector RAG)
- OpenAI API key

### Step 1: Install TAP AI App on Frappe

```bash
# Get the app
bench get-app tap_ai https://github.com/theapprenticeproject/Ai.git

# Install on site
bench --site <site-name> install-app tap_ai
```

### Step 2: Install Python Dependencies

```bash
# Install all required packages
bench pip install -r apps/tap_ai/requirements.txt

# Or install key packages individually
bench pip install langchain-openai pinecone psycopg2-binary pika redis
```

### Step 3: Install Infrastructure

```bash
# RabbitMQ (macOS)
brew install rabbitmq

# RabbitMQ (Ubuntu)
sudo apt-get install rabbitmq-server

# Redis (macOS)
brew install redis

# Redis (Ubuntu)
sudo apt-get install redis-server

# Start services
brew services start rabbitmq-server
brew services start redis-server
```

### Step 4: Set Up Pre-commit Hooks (Optional)

```bash
cd apps/tap_ai
pre-commit install
```

---

## ⚙️ Configuration

### Step 1: Add Configuration to `site_config.json`

Edit your site's `site_config.json` file and add:

```json
{
  "openai_api_key": "sk-your-openai-key-here",
  "primary_llm_model": "gpt-4o-mini",
  "embedding_model": "text-embedding-3-small",
  
  "pinecone_api_key": "pcn-your-pinecone-key-here",
  "pinecone_index": "tap-ai-byo",
  
  "rabbitmq_url": "amqp://guest:guest@localhost:5672/",
  
  "redis_host": "localhost",
  "redis_port": 6379,
  "redis_db": 0,
  
  "max_context_length": 2048,
  "vector_search_k": 5,
  "max_response_tokens": 500
}
```

### Configuration Keys Reference

| Key | Type | Purpose | Default |
|-----|------|---------|---------|
| `openai_api_key` | string | OpenAI API authentication | Required |
| `primary_llm_model` | string | Primary LLM for routing | `gpt-4o-mini` |
| `embedding_model` | string | Model for embeddings | `text-embedding-3-small` |
| `pinecone_api_key` | string | Pinecone authentication | Required |
| `pinecone_index` | string | Pinecone index name | `tap-ai-byo` |
| `rabbitmq_url` | string | RabbitMQ connection URL | `amqp://guest:guest@localhost:5672/` |
| `redis_host` | string | Redis hostname | `localhost` |
| `redis_port` | int | Redis port | `6379` |
| `redis_db` | int | Redis database number | `0` |
| `max_context_length` | int | Max LLM context tokens | `2048` |
| `vector_search_k` | int | Top-K vectors for RAG | `5` |
| `max_response_tokens` | int | Max response tokens | `500` |

### Step 2: Environment Variables (Alternative)

Create `.env` file in frappe-bench:

```bash
OPENAI_API_KEY=sk-your-key
PINECONE_API_KEY=pcn-your-key
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

> Note: A local `.env` file is included for convenience. Do not store production secrets in source control.

---

## 🧭 One-Time Setup

### Step 1: Generate the Database Schema

```bash
bench execute tap_ai.schema.generate_schema.cli
```

This creates `tap_ai_schema.json` needed by SQL and RAG engines.

### Step 2: Create Pinecone Index

```bash
bench execute tap_ai.services.pinecone_index.cli_ensure_index
```

### Step 3: Populate Pinecone Index

```bash
bench execute tap_ai.services.pinecone_store.cli_upsert_all
```

---

## 🧪 Testing

### Unified Query API (Text Example)

```bash
# Unified query: text
curl -X POST "http://localhost:8000/api/method/tap_ai.api.query.query" \
  -H "Content-Type: application/json" \
  -d '{"q": "List all courses", "user_id": "test_user"}'

# Response
{"request_id": "REQ_a1b2c3d4"}

# Poll unified result (auto long-poll defaults)
curl "http://localhost:8000/api/method/tap_ai.api.result.result?request_id=REQ_a1b2c3d4"
```

### Unified Query API (Voice Example)

```bash
# Unified query: voice
curl -X POST "http://localhost:8000/api/method/tap_ai.api.query.query" \
  -H "Content-Type: application/json" \
  -d '{"audio_url": "https://example.com/audio.mp3", "user_id": "test_user"}'

# Response
{"request_id": "VREQ_x1y2z3w4"}

# Poll unified result with explicit wait override
curl "http://localhost:8000/api/method/tap_ai.api.result.result?request_id=VREQ_x1y2z3w4&wait_seconds=25&poll_interval_ms=500"
```

### Start RabbitMQ Workers

In separate terminal windows:

```bash
# Worker 1: LLM Worker
cd frappe-bench
bench execute tap_ai.workers.llm_worker.start

# Worker 2: STT Worker
bench execute tap_ai.workers.stt_worker.start

# Worker 3: TTS Worker
bench execute tap_ai.workers.tts_worker.start
```

---

## 🌐 API Documentation

### Unified Query Endpoint

**POST** `/api/method/tap_ai.api.query.query`

Request body:
```json
{
  "q": "Your question here (text mode)",
  "user_id": "unique_user_identifier"
}
```

or

```json
{
  "audio_url": "https://example.com/audio.mp3 (voice mode)",
  "user_id": "unique_user_identifier"
}
```

Response:
```json
{
  "request_id": "REQ_abc12345"
}
```

### Unified Result Polling

**GET** `/api/method/tap_ai.api.result.result?request_id=REQ_abc12345`

Optional query params:
- `wait_seconds` (0-55)
- `poll_interval_ms` (100-2000)

If omitted, TAP AI auto-tunes defaults by mode:
- text: `wait_seconds=8`, `poll_interval_ms=300`
- voice: `wait_seconds=25`, `poll_interval_ms=500`

Response (pending):
```json
{
  "status": "processing"
}
```

Response (success):
```json
{
  "status": "success",
  "answer": "The answer to your question...",
  "query": "Your question",
  "history": [...],
  "metadata": {...}
}
```

### Legacy Voice Query Alias (Optional)

Primary endpoint:

**POST** `/api/method/tap_ai.api.query.query`

Backward-compatible alias:

**POST** `/api/method/tap_ai.api.voice_query.voice_query`

Request body:
```json
{
  "audio_url": "https://example.com/audio.mp3",
  "user_id": "unique_user_identifier"
}
```

Response:
```json
{
  "request_id": "VREQ_xyz98765"
}
```

### Legacy Voice Result Alias (Optional)

Primary endpoint:

**GET** `/api/method/tap_ai.api.result.result?request_id=VREQ_xyz98765`

Backward-compatible alias:

**GET** `/api/method/tap_ai.api.voice_result.voice_result?request_id=VREQ_xyz98765`

Response (processing):
```json
{
  "status": "processing"
}
```

Response (success):
```json
{
  "status": "success",
  "transcribed_text": "What is the first course?",
  "answer_text": "The first course is...",
  "audio_url": "/files/output_file.mp3",
  "language": "en"
}
```

> Note: `voice_result` alias may return `status: "processing"` while STT, LLM, and TTS jobs complete in the background. Poll until the final status is `success`.

---

## ⚙️ Worker System

The system uses RabbitMQ for asynchronous processing. Three workers handle different tasks:

### LLM Worker (`tap_ai/workers/llm_worker.py`)

- Pulls text queries from `text_query_queue`
- Runs the router to choose between SQL and RAG
- Manages conversation history
- Routes voice queries to TTS worker
- Updates request status in Redis cache

**Start with:**
```bash
bench execute tap_ai.workers.llm_worker.start
```

### STT Worker (`tap_ai/workers/stt_worker.py`)

- Pulls voice requests from `audio_stt_queue`
- Downloads audio from provided URL
- Uses Whisper API to transcribe
- Detects language of transcription
- Routes transcribed text to LLM worker

**Start with:**
```bash
bench execute tap_ai.workers.stt_worker.start
```

### TTS Worker (`tap_ai/workers/tts_worker.py`)

- Pulls synthesization jobs from `audio_tts_queue`
- Uses OpenAI TTS to generate speech
- Saves audio file to Frappe File Manager
- Returns audio URL and marks request as complete

**Start with:**
```bash
bench execute tap_ai.workers.tts_worker.start
```

---

## 🔍 Core File Descriptions

### API Layer

**`tap_ai/api/query.py`**
- Unified text + voice query entry point
- Rate limiting check
- Publishes to RabbitMQ `text_query_queue` (text) or `audio_stt_queue` (voice)
- Returns request_id for polling

**`tap_ai/api/result.py`**
- Unified result endpoint for text and voice
- Supports short server-side waiting to reduce coarse flow wait-node dependence
- Retrieves from Redis cache

**`tap_ai/api/voice_query.py`**
- Backward-compatible wrapper for unified query endpoint

**`tap_ai/api/voice_result.py`**
- Backward-compatible wrapper for unified result endpoint

### Services Layer

**`tap_ai/services/router.py`**
- Central query routing logic
- Chooses between SQL and RAG engines
- Manages fallback logic
- Handles chat history

**`tap_ai/services/sql_answerer.py`**
- Generates SQL from natural language
- Builds intelligent schema for LLM
- Executes queries against remote PostgreSQL
- Returns structured data

**`tap_ai/services/rag_answerer.py`**
- Retrieves semantically similar documents
- Refines queries with chat history
- Synthesizes answers from context
- Handles multi-turn conversations

**`tap_ai/services/doctype_selector.py`**
- Selects relevant DocTypes for RAG
- Reduces search space
- Improves retrieval accuracy

**`tap_ai/services/pinecone_store.py`**
- Manages Pinecone interactions
- Upserts documents with embeddings
- Performs semantic search

**`tap_ai/services/ratelimit.py`**
- Enforces API rate limits
- Uses Redis for distributed counting
- Tracks requests per user

### Workers

**`tap_ai/workers/llm_worker.py`**
- Main processing worker
- Routes queries through the dual-engine system
- Manages conversation context
- Bridges text and voice pipelines

**`tap_ai/workers/stt_worker.py`**
- Speech-to-Text processing
- Audio download and handling
- Language detection
- Whisper API integration

**`tap_ai/workers/tts_worker.py`**
- Text-to-Speech synthesis
- OpenAI TTS integration
- Frappe File Manager integration
- Audio file management

### Utilities

**`tap_ai/utils/dynamic_config.py`**
- Decouples TAP AI from TAP LMS schema changes
- Handles dynamic DocType mapping
- Manages user profiles with enrollment data
- Singleton pattern for configuration caching
- Validation and context resolution rules

**`tap_ai/utils/mq.py`**
- RabbitMQ publisher
- Queue declaration and management
- Persistent message delivery

### Infrastructure

**`tap_ai/infra/config.py`**
- Centralized configuration loader
- Frappe integration with fallbacks
- Works both inside Frappe and standalone
- Service status validation

### Schema Generation

**`tap_ai/schema/generate_schema.py`**
- Dynamically discovers all Frappe DocTypes
- Builds intelligent schema for SQL queries
- Supports admin-controlled exclusions
- Auto-detects joins and relationships

---

## 🤖 Telegram Bot Demo (Local Setup)

### Architecture Overview

```
User → Telegram → Ngrok → telegram_webhook.py → Frappe API → AI Engine
```

### Prerequisites

- Telegram account
- Ngrok installed and authenticated
- Frappe bench running

### Step 1: Create Telegram Bot

1. Search for `@BotFather` on Telegram
2. Send `/newbot`
3. Follow instructions
4. **Copy the bot token** (e.g., `123456:ABC-DEF1234`)

### Step 2: Set Up Ngrok

```bash
ngrok config add-authtoken <your-ngrok-token>
ngrok http 5000
```

Copy the HTTPS forwarding URL (e.g., `https://random-string.ngrok-free.app`)

### Step 3: Configure and Run Telegram Bridge

```bash
# Install dependencies
bench pip install Flask python-telegram-bot requests

# Edit telegram_webhook.py and set:
# - TELEGRAM_BOT_TOKEN
# - FRAPPE_API_URL
# - FRAPPE_API_KEY
# - FRAPPE_API_SECRET
# - OPENAI_API_KEY

# Run the bridge
python apps/tap_ai/telegram_webhook.py
```

### Step 4: Set Telegram Webhook

```bash
curl -F "url=https://<NGROK_URL>/webhook" \
     "https://api.telegram.org/bot<BOT_TOKEN>/setWebhook"
```

### Step 5: Test the Bot

Open Telegram and start a conversation with your bot!

---

## 📦 Deployment Guide

### Local Development

```bash
# Terminal 1: Frappe
bench start

# Terminal 2: LLM Worker
bench execute tap_ai.workers.llm_worker.start

# Terminal 3: STT Worker
bench execute tap_ai.workers.stt_worker.start

# Terminal 4: TTS Worker
bench execute tap_ai.workers.tts_worker.start

# Terminal 5: Ngrok (optional for Telegram)
ngrok http 5000
```

### Production Deployment

Use Supervisor or systemd for worker management:

```ini
# /etc/supervisor/conf.d/tap-ai-workers.conf
[program:tap-ai-llm]
command=bench execute tap_ai.workers.llm_worker.start
directory=/opt/frappe-bench
autostart=true
autorestart=true

[program:tap-ai-stt]
command=bench execute tap_ai.workers.stt_worker.start
directory=/opt/frappe-bench
autostart=true
autorestart=true

[program:tap-ai-tts]
command=bench execute tap_ai.workers.tts_worker.start
directory=/opt/frappe-bench
autostart=true
autorestart=true
```

---

## 🐛 Troubleshooting

### Issue: "OpenAI API Key not found"

```bash
# Check site_config.json
cat sites/<site-name>/site_config.json | grep openai_api_key

# Or check env vars
echo $OPENAI_API_KEY
```

### Issue: "RabbitMQ Connection Refused"

```bash
# Check if RabbitMQ is running
brew services list | grep rabbitmq

# Or check status
rabbitmqctl status

# Start if not running
brew services start rabbitmq-server
```

### Issue: "Pinecone index not found"

```bash
# Recreate index
bench execute tap_ai.services.pinecone_index.cli_ensure_index

# Upsert data
bench execute tap_ai.services.pinecone_store.cli_upsert_all
```

### Issue: Workers not processing messages

```bash
# Check RabbitMQ queues
rabbitmqctl list_queues

# Check Redis connection
redis-cli PING

# Check Frappe logs
tail -f frappe-bench/logs/frappe.log
```

---

## 📄 License

This project is licensed under the terms specified in `license.txt`.

---

**Last Updated:** 2026-03-18  
**Version:** 2.0.0  
**Author:** Anish Aman  
**Repository:** theapprenticeproject/Ai

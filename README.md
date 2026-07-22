# TAP AI - Conversational AI Engine

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Frappe](https://img.shields.io/badge/Framework-Frappe%2015-0089FF)
![OpenAI](https://img.shields.io/badge/LLM-OpenAI-412991?logo=openai&logoColor=white)
![Pinecone](https://img.shields.io/badge/VectorDB-Pinecone-00B388)
![RabbitMQ](https://img.shields.io/badge/Queue-RabbitMQ-FF6600?logo=rabbitmq&logoColor=white)
![Redis](https://img.shields.io/badge/Cache-Redis-DC382D?logo=redis&logoColor=white)
![License](https://img.shields.io/badge/license-see%20license.txt-lightgrey)

This project extends the TAP AI Frappe application with a powerful, conversational AI layer. It provides a single, robust API endpoint that can understand user questions and intelligently route them to the best tool - a curated knowledge bank, a direct database query, a semantic vector search, or a direct LLM fallback - to provide accurate, context-aware answers.

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
- [Telegram Bot Demo](#-telegram-bot-demo-local-setup)
- [Deployment Guide](#-deployment-guide)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Project Overview

**TAP AI** is a conversational AI engine built on top of the Frappe framework. It intelligently routes user queries to specialized execution engines.

### Execution Engines

| Engine | Handles | Example Queries |
|---|---|---|
| **Knowledge Bank** | Curated TAP responses, greetings, short support phrases | "Hi", "Who are you?", "I'm stuck" |
| **Text-to-SQL** | Factual, structured data queries | "Show me my TAP activities" |
| **Vector RAG** | Conceptual, semantic, summarization queries | "Explain my arts activity on creating Zentangle patterns" |
| **Direct LLM** | Open-ended conversation with no KB match | Freeform supportive replies |

### Key Features

| Feature | Description |
|---|---|
| Intelligent routing | LLM + regex fast-path selects the right engine per query |
| Multi-turn conversations | Chat history stored in Redis per user/session |
| Hybrid execution | KB → SQL → RAG → LLM with automatic fallback chain |
| Voice support | STT (Whisper) → LLM → TTS pipeline via RabbitMQ |
| Async processing | RabbitMQ workers decouple API from execution |
| Dynamic configuration | Per-deployment config via TAP LMS DocTypes |
| Admin exclusions | DocType-level exclusion system for RAG indexing |
| A/B experiment switches | `enable_doctype_profiler` and `enable_llm_router` flags for live latency experiments without code changes |
| KB vector fallback | TAP Response Knowledge indexed in Pinecone; conversational queries that slip past regex are caught by cosine similarity |

### Technology Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.10+ |
| Framework | Frappe 15 (ERPNext) |
| LLM | OpenAI GPT models |
| Embeddings | OpenAI `text-embedding-3-small` |
| Vector DB | Pinecone + pgvector |
| Database | Remote PostgreSQL (`data.evalix.xyz`) |
| Message Queue | RabbitMQ (Pika) |
| Caching | Redis (LLM responses, KB entries, chat history, routing profiles) |
| Data validation | Pydantic v2 |
| LLM orchestration | LangChain Core (`ChatPromptTemplate`, output parsers) |
| Telegram bridge | Flask + python-telegram-bot |

---

## ⚙️ Core Architecture

The system's intelligence lies in its central router, which acts as a decision-making brain. When a query is received, it follows this flow:

1. **Query Refinement:** Before any routing, the query is rewritten into a fully standalone question using the user's chat history. This resolves pronouns and follow-up references (e.g. "summarize the first one" → "summarize the video titled X") so the router and all downstream engines always receive a self-contained query. Greetings and identity queries are exempt from refinement as their meaning is always fixed.
2. **Intelligent Routing:** The refined query is first checked against fast regex patterns (zero-LLM). On a miss, an LLM determines the intent.
3. **Tool Selection:**
  - For short, curated conversational intents that match the TAP response bank, it selects the **Knowledge Bank Tool**.
  - For factual, specific questions (e.g., "list all...", "how many..."), it selects the **Text-to-SQL Engine**.
  - For conceptual, open-ended, or summarization questions (e.g., "summarize...", "explain..."), it selects the **Vector RAG Engine**.
  - For open-ended supportive conversation that does not fit the knowledge bank, it selects the **Direct LLM Tool**.
4. **Execution & Fallback:** The chosen tool executes the query. If the knowledge bank misses or returns a low-confidence match, the system falls back to the Direct LLM tool. If SQL fails to produce a satisfactory answer, the system automatically falls back to the Vector RAG engine as a safety net.
5. **Answer Synthesis:** The retrieved data or direct response is returned as a final, human-readable answer.

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

    subgraph "Routing Layer"
        Refiner["utils/query_refiner.py<br><b>Query Refiner</b><br>(rewrite follow-ups using chat history)"]
        FastPath["services/routing/routing_patterns.py<br><b>Fast Regex Router</b><br>(zero-LLM fast path)"]
        Router["services/routing/router.py<br><b>LLM-based Router</b><br>(fallback when regex misses)"]
    end

    subgraph "Services"
      KB["services/kb/direct_response_bank.py<br><b>Knowledge Bank</b>"]
        SQL["services/sql/sql_answerer.py<br><b>SQL Engine</b>"]
        RAG["services/rag/rag_answerer.py<br><b>RAG Engine</b>"]
      KBRouter["services/kb/kb_llm_router.py<br><b>KB LLM Fallback</b>"]
    end

    subgraph "Cache Layer"
        RedisLLM[("Redis<br><b>LLM Response Cache</b><br>llm_client.py · TTL 1h")]
        RedisKB[("Redis<br><b>KB Entries Cache</b><br>direct_response_bank.py · TTL 1h")]
        RedisHistory[("Redis<br><b>Chat History Cache</b><br>router.py")]
    end

    subgraph "Data Layer"
      PostgresDB[(Remote PostgreSQL<br>data.evalix.xyz)]
        PineconeDB[(Pinecone<br>Vector DB)]
    end

    User -->|Text or Voice| QueryAPI
    QueryAPI -->|Request + request_id| RabbitMQ

    RabbitMQ -->|audio_stt_queue| STTWorker
    RabbitMQ -->|text_query_queue| LLMWorker
    RabbitMQ -->|audio_tts_queue| TTSWorker

    STTWorker -->|Transcribed Text| RabbitMQ
    LLMWorker -->|Greeting / identity<br>bypass refiner| FastPath
    LLMWorker -->|Follow-up or ambiguous| Refiner
    Refiner -->|Standalone refined query| FastPath
    Refiner <-->|Cache refined queries| RedisLLM
    FastPath -->|Regex match: KB or SQL| KB
    FastPath -->|Regex miss| Router
    Router <-->|Cache routing decisions| RedisLLM
    Router -->|Curated Match| KB
    Router -->|Factual| SQL
    Router -->|Conceptual| RAG
    Router -->|KB fallback| KBRouter

    KB <-->|Read/Write KB entries| RedisKB
    KB -->|Exact match hit| LLMWorker
    KB -->|Miss / low confidence| KBRouter
    KBRouter <-->|Cache LLM KB responses| RedisLLM

    LLMWorker <-->|Read/Write chat history| RedisHistory

    SQL -->|SQL Query| PostgresDB
    RAG -->|Vector Search| PineconeDB

    LLMWorker -->|Answer| TTSWorker
    TTSWorker -->|Audio File| PostgresDB
    LLMWorker -->|Write result| RedisHistory
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
    B --> C["1. Embed Query"]
    C --> D["2. Cosine Similarity → DocType Routing"]
    D --> E["3. Parallel Pinecone Search across namespaces"]
    E --> F["4. context_preview from metadata"]
    F --> G[Rich Context Chunks]
```

**DocType routing** uses pre-built embedding profiles stored in `DoctypeRoutingProfile` (Frappe doctype) rather than a per-query LLM call. The query embedding computed for Pinecone search is reused directly — zero extra latency. See [One-Time Setup](#-one-time-setup) for bootstrapping.

##### Chunking Strategy

Vectors are built at index time by `pinecone_store.upsert_doctype`. There are three strategies:

| Strategy | Applies to | Rationale |
|---|---|---|
| **1 record → 1 vector** | Long-form content (`VideoClass`, `Quiz`, `QuizQuestion`, `Competency`, `ProjectChallenge`, etc.) | Each record is a distinct semantic unit with rich text; grouping would truncate content and dilute the embedding |
| **Semantic grouping by subject / vertical** | Structured content (`LearningUnit`, `Learning Objective`, `Assignment`, `Course Level`, etc.) | Records sharing a subject or vertical are conceptually related; co-locating them improves recall without significant precision loss |
| **Semantic grouping by relational key** | Activity / relational records (`Student`, `Teacher`, `StudentQuizAttempt`, etc.) | Records tied to the same student, assignment, or batch are always queried together |

**Semantic group config (`_SEMANTIC_GROUP_CONFIG`):**

| DocType | Groups by | Max per vector |
|---|---|---|
| **Relational / activity** | | |
| `Student` | `grade` | 8 |
| `Teacher` | `school_id` | 8 |
| `Student Assignment` | `assignment` | 8 |
| `StudentQuizAttempt` | `quiz` | 8 |
| `StudentReflection` | `student` | 6 |
| `ImgSubmission` | `assign_id` | 8 |
| `Performance` | `enrollment` | 8 |
| `Submission` | `student_assignment` | 8 |
| `LearningChoicePoint` | `student` | 6 |
| `LearningState` | `student` | 1 |
| **Content grouped by subject / vertical** | | |
| `Learning Objective` | `subject` | 6 |
| `Assignment` | `subject` | 5 |
| `LearningUnit` | `course_vertical` | 5 |
| `Course Level` | `vertical` | 5 |
| `LearningStage` | `course_level` | 5 |
| `NoteContent` | `note_type` | 5 |
| `Unit` | `course` | 5 |
| **Child doctypes** | | |
| `QuizOption` | `question_id` | 5 |

Any DocType **not** listed above defaults to **1 record per vector**. New doctypes require no code change — they index at 1:1 automatically. To enable semantic grouping for a new DocType, add one entry to `_SEMANTIC_GROUP_CONFIG` in `pinecone_store.py`.

> **Re-indexing note:** After a chunking strategy change, delete the affected namespace first (to remove stale vectors with old IDs), then re-upsert:
> ```bash
> bench execute tap_ai.services.rag.pinecone_store.cli_delete_namespace --kwargs "{'doctype': 'MyDocType'}"
> bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all --kwargs "{'doctypes': ['MyDocType']}"
> ```

#### Knowledge Bank Tool: From Curated Phrase to Direct Answer

This tool handles short, high-confidence conversational intents like greetings, acknowledgements, simple help requests, identity questions, and other curated TAP response patterns. It operates in two stages backed by Redis caching.

```mermaid
graph TD
    A[User Query] --> B["Stage 1: Load KB entries<br>(Redis cache, TTL 1h)"]
    B --> C["Normalize query + all KB candidates<br>(student_query + alternate_queries)"]
    C --> D{Exact match<br>after normalization?}
    D -->|Yes| E[Return stored TAP response<br>~50ms — no LLM]
    D -->|No| F["Stage 2: kb_llm_router.py<br>Pass full KB context to LLM"]
    F --> G{LLM: Match from KB<br>or generate answer?}
    G -->|KB match| H[Return selected KB response]
    G -->|No match| I[Return LLM-generated answer]
```

---

## 📁 Complete Codebase Structure

```
tap_ai/
├── __init__.py                          # Package initialization
├── hooks.py                             # Frappe hooks — Pinecone sync + profile refresh for all allowlisted DocTypes
├── models.py                            # Shared Pydantic v2 models (UserProfile, Enrollment, ContentDetails)
├── modules.txt                          # Module declaration
├── patches.txt                          # Database migration patches
│
├── api/                                 # REST API Endpoints
│   ├── __init__.py
│   ├── query.py                         # Unified query endpoint (text + voice, async via RabbitMQ)
│   ├── result.py                        # Unified result polling endpoint (with optional server-side wait)
│   ├── health.py                        # System health check endpoint (Redis, PostgreSQL, RabbitMQ, OpenAI)
│   ├── history.py                       # Conversation history management (clear chat history)
│   ├── metrics.py                       # RabbitMQ queue health/metrics endpoint
│   ├── wait.py                          # Delay endpoint for Glific workflow pacing
│   ├── voice_query.py                   # Backward-compatible wrapper alias for unified query
│   └── voice_result.py                  # Backward-compatible wrapper alias for unified result
│
├── services/                            # Core execution engines (grouped by domain)
│   ├── __init__.py
│   ├── rag/                             # Vector RAG engine
│   │   ├── rag_answerer.py              # RAG answer synthesis (query refine → search → synthesize)
│   │   └── pinecone_store.py            # Pinecone vector store (upsert, parallel search, auto-sync hooks)
│   ├── sql/                             # Text-to-SQL engine
│   │   ├── sql_answerer.py              # SQL generation → execution → answer synthesis
│   │   └── doctype_selector.py          # LLM-based DocType selector (fallback when profiles unavailable)
│   ├── kb/                              # Knowledge Bank engine
│   │   ├── direct_response_bank.py      # Exact-match KB lookup and Redis cache
│   │   └── kb_llm_router.py             # LLM fallback when no exact KB match
│   └── routing/                         # Router and fast-path patterns
│       ├── router.py                    # Intelligent router (brain of system)
│       ├── routing_patterns.py          # Regex fast-path patterns (zero-LLM routing)
│       └── doctype_profiler.py          # Embedding-based DocType routing profiles (generate, store, route)
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
│   ├── llm_client.py                    # Shared LLM client (singleton + Redis response cache)
│   ├── sql_catalog.py                   # Schema catalog loader
│   └── pinecone_index.py                # Pinecone index lifecycle
│
├── utils/                               # Utility functions
│   ├── __init__.py
│   ├── dynamic_config.py                # Dynamic config for TAP LMS integration (returns Pydantic models)
│   ├── remote_db.py                     # Remote PostgreSQL connection pool and query helpers
│   ├── mq.py                            # RabbitMQ publisher utility
│   ├── prompt_bank.py                   # Prompt Suggestion loader and system-message renderer
│   ├── prompt_suggestions.json          # Default prompt suggestions (fallback when no DocType)
│   ├── query_refiner.py                 # Rewrites follow-up queries into standalone questions (LCEL chain)
│   └── ratelimit.py                     # API rate limiting utility
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
└── tap_ai/                              # Frappe DocTypes and dashboards
    ├── doctype/
    │   ├── doctype_routing_profile/     # Persistent store for DocType embedding profiles
    │   └── ...                          # TAP Response Knowledge, AI Knowledge Base, etc.
    ├── dashboard_chart/                 # Analytics dashboard chart definitions
    ├── number_card/                     # Analytics dashboard number card definitions
    └── tap_ai_dashboard/                # TAP AI Analytics dashboard configuration

├── tests/                               # Test suite
│   ├── conftest.py                      # pytest path/import bootstrap
│   ├── test_routing_patterns.py         # Routing pattern unit tests
│   ├── test_remote_db.py                # Remote DB connectivity tests
│   └── test_sql_sanitization.py         # SQL sanitization tests
│
├── scripts/                             # Standalone scripts and integrations
│   └── telegram_webhook.py              # Telegram bot bridge (Flask, reads .env)
│
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
└── __init__.py                          # Root package init
```

---

## 📦 Dependencies

All runtime dependencies are in `requirements.txt`. Frappe is installed separately via bench.

| Package | Version | Purpose |
|---|---|---|
| `pika` | latest | RabbitMQ client for async worker messaging |
| `openai` | ≥1.40.0 | GPT routing, Whisper STT, TTS synthesis |
| `langchain-core` | ≥0.3.0 | `ChatPromptTemplate`, `StrOutputParser`, `JsonOutputParser`, `MessagesPlaceholder` |
| `langchain-openai` | ≥0.1.17 | `ChatOpenAI` and `OpenAIEmbeddings` wrappers |
| `pydantic` | ≥2.0 | Shared input/output models (`UserProfile`, `Enrollment`, `ContentDetails`) |
| `pinecone` | latest | Vector database client for RAG retrieval |
| `psycopg2-binary` | latest | PostgreSQL driver for remote DB access |
| `requests` | latest | HTTP client used by STT worker to download audio |
| `loguru` | ≥0.7.2 | Structured logging across all services |
| `tenacity` | ≥9.0.0 | Retry logic for transient LLM/network errors |
| `Frappe` | ~15.0+ | Framework — installed via bench, not requirements.txt |

**Telegram bot** (`telegram_webhook.py`) requires `Flask` and `python-telegram-bot` installed separately — not included in `requirements.txt`.

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
  "rag_vector_backend": "pinecone",
  
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
| `primary_llm_model` | string | Primary LLM for routing and SQL | `gpt-4o-mini` |
| `profiler_summary_model` | string | LLM used for DocType profile summary generation (one-time) | `gpt-4o` |
| `embedding_model` | string | Model for embeddings | `text-embedding-3-small` |
| `pinecone_api_key` | string | Pinecone authentication | Required |
| `pinecone_index` | string | Pinecone index name | `tap-ai-byo` |
| `rag_vector_backend` | string | RAG backend selector: `pinecone`, `pgvector`, or `both` | `pinecone` |
| `rabbitmq_url` | string | RabbitMQ connection URL | `amqp://guest:guest@localhost:5672/` |
| `redis_host` | string | Redis hostname | `localhost` |
| `redis_port` | int | Redis port | `6379` |
| `redis_db` | int | Redis database number | `0` |
| `max_context_length` | int | Max LLM context tokens | `2048` |
| `vector_search_k` | int | Top-K vectors for RAG | `5` |
| `max_response_tokens` | int | Max response tokens | `500` |
| `rag_max_context_hits` | int | Max Pinecone hits used for context building | `6` |
| `rag_synthesis_model` | string | LLM model for RAG answer synthesis | `gpt-4o-mini` |
| `rag_synthesis_max_tokens` | int | Max tokens for RAG answer | `500` |
| `enable_doctype_profiler` | bool | **A/B switch.** When `false`, bypasses cosine-similarity namespace routing and queries all allowlisted DocTypes — useful for latency experiments. | `true` |
| `enable_llm_router` | bool | **A/B switch.** When `false`, queries that don't match fast regex patterns go straight to `vector_search` with no LLM call. | `true` |

### Step 2: Environment Variables (Alternative)

Create `.env` file in frappe-bench:

```bash
OPENAI_API_KEY=sk-your-key
PINECONE_API_KEY=pcn-your-key
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RAG_VECTOR_BACKEND=pinecone
```

> Note: A local `.env` file is included for convenience. Do not store production secrets in source control.

---

## 🧭 One-Time Setup

### Step 1: Generate the Database Schema

```bash
bench execute tap_ai.schema.generate_schema.cli
```

This creates `tap_ai_schema.json` needed by SQL and RAG engines.

### Step 2: Run pgvector migration

```bash
bench migrate
```

This runs the new patch that creates the pgvector table and indexes in the remote PostgreSQL database.

If you see an error like `could not open extension control file ... vector.control`, the remote PostgreSQL host is missing the pgvector package. Install pgvector on that server first, then rerun the migration.

For PostgreSQL 14, the package is usually `postgresql-14-pgvector`.

### Step 3: Create Pinecone Index

```bash
bench execute tap_ai.infra.pinecone_index.cli_ensure_index
```

### Step 4: Populate Pinecone Index

```bash
bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all
```

### Step 5: Populate pgvector in parallel

```bash
bench execute tap_ai.services.rag.pgvector_store.cli_upsert_all
```

### Step 4: Bootstrap DocType Routing Profiles

This generates a topic-aware embedding profile for each allowlisted DocType. The profiles are used to route queries to the right Pinecone namespaces without an LLM call at query time.

```bash
bench execute tap_ai.services.routing.doctype_profiler.generate_all_profiles
```

> **Local development only:** if running locally without a direct connection to the remote PostgreSQL, ensure the DB tunnel is open in a separate terminal before running this command. On the production server (`ai.evalix.xyz`) the remote DB is directly reachable and no tunnel is needed.

This is a **one-time operation**. After that, the `doc_events` hook automatically refreshes any DocType's profile in the background whenever a record is inserted or updated. Profiles are stored in the `Doctype Routing Profile` Frappe doctype (persistent) and Redis (7-day TTL cache). A Redis flush does **not** trigger regeneration — profiles reload from the Frappe doctype in ~50ms.

**Re-generate a single DocType profile** (e.g. after a schema change):
```bash
bench execute tap_ai.services.routing.doctype_profiler.generate_doctype_profile \
  --kwargs "{'doctype': 'VideoClass'}"
```

**Control which model generates summaries** (default: `gpt-4o`):
```json
"profiler_summary_model": "gpt-4o"
```

### Step 6: Bootstrap Knowledge Bank in both vector stores

The `TAP Response Knowledge` doctype is indexed in both Pinecone and pgvector so conversational queries that slip past the fast regex patterns can still be routed there via cosine similarity.

**5a. Generate the KB routing profile** (hand-crafted summary — no LLM needed):
```bash
bench execute tap_ai.services.routing.doctype_profiler.generate_kb_profile
```

**6b. Index all active KB entries into Pinecone:**
```bash
bench execute tap_ai.services.rag.pinecone_store.upsert_kb_entries
```

**6c. Index all active KB entries into pgvector:**
```bash
bench execute tap_ai.services.rag.pgvector_store.upsert_kb_entries
```

After the initial load, every KB save/delete triggers incremental sync to both backends automatically via `doc_events` hooks — no manual re-run needed.

### A/B Experiment Switches

Two feature flags let you toggle major routing decisions live via `bench set-config` and a worker restart — no code deployment needed.

| Flag | Default | Effect when `false` |
|---|---|---|
| `enable_doctype_profiler` | `true` | Skips cosine-similarity namespace routing; queries all 35 allowlisted DocTypes in parallel |
| `enable_llm_router` | `true` | Queries that don't match fast regex go straight to `vector_search` with no LLM call |

Set `rag_vector_backend` to `both` to merge Pinecone and pgvector search results in the RAG path for comparison.

Both flags are surfaced in every response under `metadata.profiler_enabled` / `metadata.llm_router_enabled` so latency can be compared directly from the response JSON.

```bash
# Example: disable LLM router for A/B test
bench --site ai.all set-config enable_llm_router false
bench --site ai.all restart

# Re-enable
bench --site ai.all set-config enable_llm_router true
bench --site ai.all restart
```

> **Benchmark result (2026-05-31):** Profiler ON vs OFF on `"What is Zentangle arts?"`:
> - Profiler ON → 1,004 ms vector search, 2,330 ms total (5 namespaces)
> - Profiler OFF → 3,916 ms vector search, 6,215 ms total (35 namespaces)
> - **Profiler ON is ~2.7× faster end-to-end.** Keep it on in production.

### Pinecone Maintenance Commands

**Re-index a single DocType in Pinecone:**
```bash
bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all \
  --kwargs "{'doctypes': ['VideoClass']}"
```

**Re-index a single DocType in pgvector:**
```bash
bench execute tap_ai.services.rag.pgvector_store.cli_upsert_all \
  --kwargs "{'doctypes': ['VideoClass']}"
```

**Run the pgvector migration directly:**
```bash
bench execute tap_ai.services.rag.pgvector_store.cli_migrate_pgvector
```

**Compare Pinecone vs pgvector on the same queries:**
```bash
bench execute tap_ai.services.rag.pgvector_store.cli_compare_backends \
  --kwargs "{'queries': ['What is financial literacy?', 'Summarize the arts activity on Zentangle'], 'k': 5}"
```

**Delete a namespace before re-indexing** (required when a DocType's chunking strategy changes — otherwise stale vectors accumulate):
```bash
bench execute tap_ai.services.rag.pinecone_store.cli_delete_namespace \
  --kwargs "{'doctype': 'QuizQuestion'}"
```

Then re-upsert:
```bash
bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all \
  --kwargs "{'doctypes': ['QuizQuestion']}"
```

Do the same for pgvector if you changed chunking there:
```bash
bench execute tap_ai.services.rag.pgvector_store.cli_delete_namespace \
  --kwargs "{'doctype': 'QuizQuestion'}"
bench execute tap_ai.services.rag.pgvector_store.cli_upsert_all \
  --kwargs "{'doctypes': ['QuizQuestion']}"
```

**Delete and recreate the entire index** (full reset):
```bash
bench execute tap_ai.infra.pinecone_index.cli_delete_index
bench execute tap_ai.infra.pinecone_index.cli_ensure_index
bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all
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

### Automated Tests

```bash
# Run the full test suite
cd apps/tap_ai
pytest -v

# Run a specific module
pytest tests/test_routing_patterns.py -v
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

### Health Check

**GET** `/api/method/tap_ai.api.health.health`

Returns connectivity status of all external dependencies. HTTP 200 when healthy; HTTP 503 when any dependency is down.

Response:
```json
{
  "status": "ok",
  "timestamp": 1716633600,
  "checks": {
    "redis":    { "status": "ok", "latency_ms": 2 },
    "postgres": { "status": "ok", "latency_ms": 5 },
    "rabbitmq": { "status": "ok", "latency_ms": 8 },
    "openai":   { "status": "ok" }
  }
}
```

### Clear Conversation History

**POST** `/api/method/tap_ai.api.history.clear`

Clears the Redis chat history for a user so the next query starts a fresh conversation.

Request body:
```json
{
  "user_id": "unique_user_identifier",
  "session_id": "optional_session_id"
}
```

Response:
```json
{
  "status": "ok",
  "user_id": "unique_user_identifier",
  "session_id": null,
  "cleared_key": "tap_ai:history:unique_user_identifier"
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
bench execute tap_ai.infra.pinecone_index.cli_ensure_index

# Upsert data
bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all
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

**Last Updated:** 2026-05-31
**Version:** 2.2.0
**Author:** Anish Aman
**Repository:** theapprenticeproject/Ai

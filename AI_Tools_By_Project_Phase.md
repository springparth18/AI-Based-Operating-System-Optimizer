# AI Tools & Dev Tools — AI OS Optimizer Project

> Recommended tools mapped to each phase of the project to maximize efficiency and pace.

---

## Legend

| Badge | Meaning |
|-------|---------|
| 🤖 AI | AI-powered tool |
| 🛠 Dev | Developer / build tool |
| 📊 Observability | Monitoring / metrics tool |

---

## Part 1 — Environment Setup

> Docker infra, Kafka topics, PostgreSQL schema.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Claude Code** | Generate docker-compose, Kafka config, and SQL schema from descriptions. Saves hours of config boilerplate. |
| **GitHub Copilot** | Inline completions for pom.xml, requirements.txt, and YAML config files as you type. |
| **Perplexity AI** | Fast research for version compatibility between Java 21, Kafka 7.x, and Spring Boot 3.x. |

### 🛠 Dev Tools

| Tool | Why Use It |
|------|-----------|
| **Docker Desktop** | Manage Kafka + Zookeeper + PostgreSQL containers with a visual dashboard. |
| **DBeaver** | GUI client to inspect and query PostgreSQL tables; good for validating schema after migrations. |
| **GitHub + Actions** | Monorepo management. Set up CI early to catch build breaks across services. |

---

## Part 2–3 — Monitoring Agent & Ingestion API

> Java OSHI monitoring agent + Spring Boot telemetry ingestion layer.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Claude / Copilot** | Generate boilerplate Java classes (TelemetrySnapshot, OSHI collectors, OkHttp publisher) from specs. |
| **Cursor IDE** | AI-native IDE — great for navigating a multi-module Maven project and refactoring Spring Boot services. |
| **Codeium** | Free Copilot alternative — useful if you want AI completions in IntelliJ without a subscription. |

### 🛠 Dev & Testing Tools

| Tool | Why Use It |
|------|-----------|
| **Postman / Bruno** | Test your Spring Boot /api/telemetry endpoint manually before writing consumers. |
| **Spring Actuator** | Expose /health and /metrics on the ingestion API from day one to monitor service health. |
| **Kafka UI** | Visual browser for Kafka topics — confirm telemetry messages are landing in telemetry-topic. |

---

## Part 4 — Storage & Streaming (Kafka + PostgreSQL)

> Dual-path data flow: PostgreSQL time-series storage + live Kafka stream consumers.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Claude** | Generate Python Kafka consumer code, psycopg2 batch insert logic, and index optimization queries. |
| **Aiven AI** | Managed Kafka + PostgreSQL with AI query suggestions — removes infra ops if you want a cloud option. |

### 📊 Observability Tools

| Tool | Why Use It |
|------|-----------|
| **Kafka UI (Provectus)** | Monitor consumer lag, partition offsets, and message throughput across all 5 Kafka topics. |
| **pgAdmin 4** | Full PostgreSQL management — run EXPLAIN ANALYZE on time-series queries to validate your indexes. |
| **Prometheus + Grafana** | Instrument Kafka consumer lag and DB write latency early — you'll need these numbers for the paper. |

---

## Part 5–7 — ML Analytics & Prediction Engine

> Python analytics: Isolation Forest, LOF, One-Class SVM anomaly detection + ARIMA forecasting.

### 🤖 AI / ML Tools

| Tool | Why Use It |
|------|-----------|
| **Claude** | Explain model hyperparameter choices, generate sklearn pipeline code, and help interpret precision/recall results. |
| **Jupyter + JupyterAI** | Explore telemetry data interactively. JupyterAI adds Claude/GPT-4 inside notebooks for inline help. |
| **Weights & Biases** | Track Isolation Forest contamination param experiments. Log all model runs for your research paper eval. |
| **SHAP** | Explainable AI library — generate feature importance plots for the XAI layer in your paper. |

### 🛠 Dev Tools

| Tool | Why Use It |
|------|-----------|
| **UV (package manager)** | Ultra-fast Python package installer — much faster than pip for installing sklearn, statsmodels, pandas. |
| **Evidently AI** | Monitor ML model drift over time — alerts when anomaly detection accuracy degrades. |

---

## Part 8 — LLM Root Cause Analysis Engine

> Ollama + Llama 3 powered root cause analysis engine with XAI dependency graphs.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Ollama** | Run Llama 3 (8B) locally — zero latency cost, privacy-safe, no API bills. Your primary RCA engine. |
| **OpenAI GPT-4o** | Fallback for when local Llama 3 quality isn't enough. Use API for edge-case RCA validation during eval. |
| **LangChain / LlamaIndex** | Structure your LLM prompts with output parsers, retry logic, and JSON extraction for RCA responses. |
| **PromptFoo** | Automated LLM prompt evaluation — test your RCA prompts against 20+ anomaly scenarios and compare outputs. |

### 📊 Observability Tools

| Tool | Why Use It |
|------|-----------|
| **Ollama Web UI** | Test Llama 3 RCA prompts manually before wiring into the service pipeline. |
| **Phoenix (Arize)** | Open-source LLM observability — trace every RCA call, token counts, and latency for paper benchmarks. |

---

## Part 9–12 — Recommendation, Policy, Execution & Feedback

> Recommendation engine, policy/safety validation, autonomous fix execution, feedback learning loop.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Claude** | Generate policy rule sets, risk classification logic, and rollback strategy code. |
| **Optuna** | Hyperparameter tuning for ML-based recommendation ranking — automates priority_score optimization. |
| **River ML** | Online/incremental ML library — ideal for the feedback learning engine that retrains from outcomes. |

### 🛠 Dev & Safety Tools

| Tool | Why Use It |
|------|-----------|
| **OPA (Open Policy Agent)** | Declarative policy engine — define action whitelist/blacklist rules in Rego instead of hardcoded Java. |
| **Prefect / Celery** | Task queue for the autonomous execution engine — handles scheduling, retries, and rollback orchestration. |

---

## Part 13 — React Dashboard

> React + WebSocket real-time dashboard with health score, anomaly feed, and XAI dependency graphs.

### 🤖 AI / Design Tools

| Tool | Why Use It |
|------|-----------|
| **v0 by Vercel** | Generate React dashboard components from text prompts — great for the health score widget and charts. |
| **Claude** | Prototype React component layouts and Recharts configs — paste in your data schema for context. |
| **Figma AI** | Design the dashboard layout before coding it. AI can auto-generate component specs from your wireframes. |

### 🛠 Dev & UI Libraries

| Tool | Why Use It |
|------|-----------|
| **Recharts** | Recommended in your guide — composable chart library that works cleanly with React and WebSocket data. |
| **Shadcn/ui** | Unstyled component library — gives you table, card, badge, and dialog components without design overhead. |
| **React Query** | Cache and sync server state alongside WebSocket streams — handles reconnection and stale data gracefully. |
| **Storybook** | Develop and test dashboard components (anomaly card, health gauge) in isolation before wiring to WebSocket. |

---

## Part 14–15 — Integration Testing, Benchmarks & Research Paper

> Integration tests, latency benchmarks, Docker full-stack deployment, and Scopus conference paper.

### 🤖 AI Tools

| Tool | Why Use It |
|------|-----------|
| **Claude** | Draft research paper sections, generate abstract, and format IEEE references from your experiment notes. |
| **Writefull** | Academic writing polish — built for research papers with journal-aware language suggestions. |
| **CodiumAI** | Auto-generate unit tests for Python anomaly detection and policy validation functions. |

### 🛠 Testing & Performance Tools

| Tool | Why Use It |
|------|-----------|
| **k6 / Locust** | Load test the Spring Boot ingestion API at scale — generate latency numbers for your paper's benchmark table. |
| **Grafana + Prometheus** | End-to-end latency dashboard — measure all 7 pipeline stages you need for the benchmark table. |
| **Docker Scout** | Scan your service images for vulnerabilities before the final demo deployment. |

---

## High-Impact Tool Picks (Summary)

| Tool | Phase | Why It Matters Most |
|------|-------|---------------------|
| **Claude Code / Cursor** | All phases | 10+ services in 3 languages — AI-assisted coding is essential |
| **Weights & Biases** | Parts 5–7 | Log all ML experiments; essential for paper's evaluation section |
| **PromptFoo** | Part 8 | Validate RCA prompts across 20+ scenarios before final demo |
| **Grafana + Prometheus** | Parts 4–14 | Collect latency_p50/p95 numbers needed for the paper benchmark table |
| **SHAP** | Parts 5–8 | XAI feature importance plots — a direct research contribution |
| **Writefull** | Part 15 | Academic-grade writing polish for the Scopus paper |
| **River ML** | Part 12 | Online learning fits the feedback loop architecture perfectly |

---

*Generated for: AI-Based Operating System Optimizer — Final Year B.Tech (CS)*

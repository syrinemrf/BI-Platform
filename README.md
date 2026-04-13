# BI Platform v3.0 — LLM-Powered Enterprise ETL

A research-grade Business Intelligence platform featuring an **LLM-powered ETL pipeline** that automatically transforms heterogeneous data sources into star schema warehouses. Accompanies the research paper *"LLM-Powered Enterprise ETL: A Confidence-Gated, Human-in-the-Loop Approach to Automated Star Schema Generation"*.

![Version](https://img.shields.io/badge/version-3.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![React](https://img.shields.io/badge/react-18+-blue.svg)
![Tests](https://img.shields.io/badge/tests-66%20passed-brightgreen.svg)
![Coverage](https://img.shields.io/badge/coverage-82%25-yellowgreen.svg)

## Key Innovations (from the paper)

| # | Innovation | Description |
|---|---|---|
| 1 | **Confidence-Gated Routing** | Dynamically selects local Llama 3 vs cloud Claude based on task complexity |
| 2 | **Adaptive Few-Shot Memory** | FAISS vector store of human-approved mappings improves accuracy over time |
| 3 | **Schema Fingerprinting** | Deterministic SHA-256 hash for O(1) drift detection across runs |
| 4 | **Divide-Verify-Refine** | Self-correcting SQL generation with SQLFluff static analysis feedback |

## Features

### Core BI Platform
- **Multi-format Upload**: CSV, Excel (.xlsx/.xls), JSON, API endpoints
- **Auto Schema Detection**: Measures, dimensions, keys, entity types identified automatically
- **Star Schema Generation**: Fact + dimension tables with surrogate keys and time dimensions
- **ETL Pipeline**: Data cleaning, quality validation (completeness, uniqueness, validity, consistency)
- **Data Warehouse**: SQLite (dev) / PostgreSQL (prod) with DDL generation
- **Interactive Dashboards**: KPIs, time series, aggregations, dynamic filtering
- **Natural Language Queries**: Ask questions in plain English/French via local LLaMA 3
- **Bilingual UI**: Complete English and French support
- **Auth System**: Optional accounts to save projects; full guest mode for anonymous use

### LLM ETL Pipeline (NEW)
- **5-Layer Architecture**: Ingestion → Profiling → LLM Agents → HITL Validation → Star Schema Loading
- **Multi-Source Ingestion**: CSV, JSON, XML, Excel, PDF text extraction
- **Schema Profiling**: Automatic column classification, null analysis, cardinality, statistical distributions
- **Schema Drift Detection**: Fingerprint-based tracking across pipeline runs
- **3 LLM Agents**: Schema Mapper, Cleaning Rules Generator, ETL Code Generator
- **Human-in-the-Loop**: Confidence thresholding with review queue for uncertain mappings
- **Data Quality Gates**: Completeness, uniqueness, row count, numeric range validation
- **Full Data Lineage**: Step-by-step tracking with Markdown/JSON export
- **Benchmarking Framework**: Precision/recall/F1 against ground truth with result tables

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       5-Layer ETL Pipeline                        │
├──────────────┬──────────────┬──────────────┬──────────────┬──────┤
│ L1: Ingest   │ L2: Profile  │ L3: Agents   │ L4: HITL     │ L5:  │
│ CSV/JSON/XML │ Schema +     │ Mapper +     │ Confidence   │ Star │
│ Excel/PDF    │ Fingerprint  │ Cleaner +    │ Gating +     │Load  │
│              │ + Drift      │ CodeGen      │ Review Queue │+ DDL │
└──────┬───────┴──────┬───────┴──────┬───────┴──────┬───────┴──┬───┘
       │              │              │              │          │
       v              v              v              v          v
┌──────────────┐ ┌─────────┐ ┌────────────┐ ┌──────────┐ ┌────────┐
│ DataFrame    │ │ Schema  │ │ FAISS RAG  │ │ Review   │ │ SQLite │
│ (Pandas)     │ │ Context │ │ Vector     │ │ Queue    │ │ Warehouse│
│              │ │         │ │ Store      │ │          │ │        │
└──────────────┘ └─────────┘ └────────────┘ └──────────┘ └────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    v              v              v
              ┌──────────┐ ┌──────────┐ ┌──────────────┐
              │ Ollama   │ │ Claude   │ │ MiniLM-L6-v2 │
              │ Llama 3  │ │ (cloud)  │ │ Embeddings   │
              └──────────┘ └──────────┘ └──────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- (Optional) Docker, Ollama for LLM features

### Local Development

#### Backend

```bash
cd bi-platform/backend
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows
pip install -r requirements.txt
python main.py
```

Backend: http://localhost:8000 | Docs: http://localhost:8000/docs

#### Frontend

```bash
cd bi-platform/frontend
npm install
npm run dev
```

Frontend: http://localhost:5173

#### (Optional) Ollama

```bash
ollama serve
ollama pull llama3:8b
```

### Docker Deployment

```bash
cd bi-platform
docker-compose up --build
```

| Service | URL |
|---|---|
| Frontend | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| PostgreSQL | localhost:5432 |
| Ollama | http://localhost:11434 |

## Project Structure

```
bi-platform/
├── backend/
│   ├── api/routes/          # auth, datasets, etl, warehouse, dashboard, llm
│   │   └── etl_llm/        # pipeline_routes, hitl_routes
│   ├── core/                # database, ORM models, Pydantic schemas
│   ├── services/
│   │   ├── etl_llm/        # LLM ETL pipeline modules
│   │   │   ├── profiling/   # ingestion, schema_profiler, drift_detector
│   │   │   ├── rag/         # schema_store (FAISS vector store)
│   │   │   ├── agents/      # schema_mapper, cleaning_agent, code_generator
│   │   │   ├── validation/  # hitl_validator, quality_gates
│   │   │   ├── lineage/     # lineage_tracker
│   │   │   ├── loader/      # star_schema_loader
│   │   │   ├── orchestrator/# pipeline_orchestrator
│   │   │   └── evaluation/  # benchmark, ground_truth
│   │   └── ...              # auth, data_quality, ddl_generator, etl_pipeline, llm, schema_analyzer
│   ├── tests/etl_llm/      # 66 tests (82% coverage)
│   ├── utils/               # file handlers, validators
│   ├── main.py
│   ├── config.py
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── components/      # Layout, Dashboard, DataUpload, common
│   │   ├── pages/           # Auth, Dashboard, DataSources, ETL, ETLLLMPage, Schema, Warehouse, Settings, About, 404
│   │   ├── store/           # Redux (auth, ui, data)
│   │   ├── services/        # API client with JWT interceptor
│   │   ├── locales/         # en.json, fr.json
│   │   └── types/
│   ├── package.json
│   └── Dockerfile
├── paper/paper.tex          # IEEEtran research article
├── sql/init.sql
├── ollama/Modelfile
├── docker-compose.yml
└── README.md
```

## API Endpoints

### Authentication
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/auth/register` | Create account |
| POST | `/api/auth/login` | Login |
| GET | `/api/auth/me` | Current user profile |
| GET | `/api/auth/projects` | List user projects |
| POST | `/api/auth/projects` | Save project |
| PUT | `/api/auth/projects/{id}` | Update project |
| DELETE | `/api/auth/projects/{id}` | Delete project |

### Datasets
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/datasets/upload` | Upload file |
| GET | `/api/datasets` | List all |
| GET | `/api/datasets/{id}/preview` | Preview rows |
| GET | `/api/datasets/{id}/schema` | Schema analysis |
| DELETE | `/api/datasets/{id}` | Delete |

### ETL
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/etl/run` | Run pipeline |
| GET | `/api/etl/status/{job_id}` | Check progress |
| POST | `/api/etl/quality-check/{id}` | Quality report |

### Warehouse
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/warehouse/tables` | List tables |
| POST | `/api/warehouse/query` | Execute SQL |
| GET | `/api/warehouse/schema` | Star schema |

### Dashboard
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/dashboard/kpis` | KPIs |
| GET | `/api/dashboard/timeseries` | Time series |
| GET | `/api/dashboard/filters` | Filter options |

### LLM
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/llm/status` | Availability |
| POST | `/api/llm/query` | NL to SQL |
| POST | `/api/llm/natural-query` | Query + execute |

### LLM ETL Pipeline
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/etl-llm/run` | Run full LLM ETL pipeline |
| GET | `/api/etl-llm/status/{id}` | Pipeline run status |
| GET | `/api/etl-llm/lineage/{id}` | Data lineage graph |
| GET | `/api/etl-llm/documentation/{id}` | Auto-generated docs |
| GET | `/api/etl-llm/hitl/review-queue` | Pending HITL reviews |
| POST | `/api/etl-llm/hitl/approve/{id}` | Approve review item |
| POST | `/api/etl-llm/hitl/reject/{id}` | Reject review item |
| POST | `/api/etl-llm/hitl/modify/{id}` | Modify and approve |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./bi_warehouse.db` | Database connection |
| `SECRET_KEY` | (built-in) | JWT signing key - **change in production** |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server |
| `OLLAMA_MODEL` | `llama3:8b` | LLM model |
| `ANTHROPIC_API_KEY` | *(none)* | Cloud fallback for confidence-gated routing |
| `CONFIDENCE_THRESHOLD` | `0.7` | CGR threshold for local vs cloud routing |
| `HITL_AUTO_APPROVE_THRESHOLD` | `0.85` | Auto-approve confidence threshold |
| `HITL_AUTO_REJECT_THRESHOLD` | `0.3` | Auto-reject confidence threshold |
| `FAISS_INDEX_PATH` | `./faiss_store/` | Path for FAISS vector index persistence |
| `DEBUG` | `true` | Debug mode |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `1440` | JWT token lifetime |

## Usage

1. **Upload** — Go to Data Sources, upload CSV/Excel/JSON
2. **ETL** — Configure and run the pipeline with quality checks
3. **LLM ETL** — Use the ETL LLM page for automated star schema generation:
   - Upload a data file and select source type
   - The pipeline runs all 5 layers automatically
   - Review HITL queue for uncertain mappings
   - Inspect lineage and generated tables
4. **Schema** — Review generated star schema (fact + dimensions)
5. **Dashboard** — Explore KPIs, charts, time series
6. **Query** — Use SQL or natural language in the Warehouse
7. **Save** — Create an account to save your projects

## Testing

```bash
cd backend

# Run all ETL LLM tests
python -m pytest tests/etl_llm/ -v

# Run with coverage
python -m pytest tests/etl_llm/ --cov=services/etl_llm --cov-report=term-missing

# Run specific test module
python -m pytest tests/etl_llm/test_integration.py -v
```

Current status: **66 tests passing**, **82% coverage** on `services/etl_llm`.

## Research Paper

The accompanying paper is located at `paper/paper.tex` (IEEEtran format). To compile:

```bash
cd paper
pdflatex paper.tex
# Run twice for references
pdflatex paper.tex
```

## Localization

Full support for English and French. Toggle via the header button or Settings page.

## License

MIT

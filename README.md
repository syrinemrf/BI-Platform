# BI Platform v2.0

A complete Business Intelligence platform that transforms raw data into structured star schemas and interactive dashboards, powered by a local AI (LLaMA 3).

![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![React](https://img.shields.io/badge/react-18+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)

## Features

- **Multi-format Upload**: CSV, Excel (.xlsx/.xls), JSON, API endpoints
- **Auto Schema Detection**: Measures, dimensions, keys, entity types identified automatically
- **Star Schema Generation**: Fact + dimension tables with surrogate keys and time dimensions
- **ETL Pipeline**: Data cleaning, quality validation (completeness, uniqueness, validity, consistency)
- **Data Warehouse**: SQLite (dev) / PostgreSQL (prod) with DDL generation
- **Interactive Dashboards**: KPIs, time series, aggregations, dynamic filtering
- **Natural Language Queries**: Ask questions in plain English/French via local LLaMA 3
- **Bilingual UI**: Complete English and French support
- **Auth System**: Optional accounts to save projects; full guest mode for anonymous use
- **About & FAQ**: In-app documentation explaining the platform

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Frontend     │────>│    Backend      │────>│   PostgreSQL    │
│  React + Vite   │     │    FastAPI      │     │   Data Warehouse│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               v
                        ┌─────────────────┐
                        │     Ollama      │
                        │   LLaMA 3 8B    │
                        └─────────────────┘
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
│   ├── core/                # database, ORM models, Pydantic schemas
│   ├── services/            # ETL, schema analysis, star schema, data quality, auth, LLM
│   ├── utils/               # file handlers, validators
│   ├── main.py
│   ├── config.py
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── components/      # Layout, Dashboard, DataUpload, common
│   │   ├── pages/           # Auth, Dashboard, DataSources, ETL, Schema, Warehouse, Settings, About, 404
│   │   ├── store/           # Redux (auth, ui, data)
│   │   ├── services/        # API client with JWT interceptor
│   │   ├── locales/         # en.json, fr.json
│   │   └── types/
│   ├── package.json
│   └── Dockerfile
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

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./bi_warehouse.db` | Database connection |
| `SECRET_KEY` | (built-in) | JWT signing key - **change in production** |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server |
| `OLLAMA_MODEL` | `llama3:8b` | LLM model |
| `DEBUG` | `true` | Debug mode |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `1440` | JWT token lifetime |

## Usage

1. **Upload** - Go to Data Sources, upload CSV/Excel/JSON
2. **ETL** - Configure and run the pipeline with quality checks
3. **Schema** - Review generated star schema (fact + dimensions)
4. **Dashboard** - Explore KPIs, charts, time series
5. **Query** - Use SQL or natural language in the Warehouse
6. **Save** - Create an account to save your projects

## Localization

Full support for English and French. Toggle via the header button or Settings page.

## License

MIT

# BI Platform

A complete Business Intelligence platform with automatic schema detection, ETL pipeline, star schema generation, and LLM-powered analytics.

![BI Platform](https://img.shields.io/badge/version-1.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![React](https://img.shields.io/badge/react-18+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)

## Features

### Data Ingestion
- Upload CSV, Excel (xlsx, xls), and JSON files
- Automatic schema detection
- Support for multiple datasets

### Schema Analysis
- Automatic detection of measures (numeric) and dimensions (categorical)
- Date column identification for time dimensions
- Primary/foreign key detection
- Entity suggestions (customer, product, time, location, etc.)

### ETL Pipeline
- **Extract**: Load from various file formats
- **Transform**:
  - Handle missing values (drop, fill mean/median/mode)
  - Remove duplicates
  - Normalize strings
  - Generate dimension tables with surrogate keys
  - Generate fact table with foreign keys
- **Load**: Insert into PostgreSQL data warehouse
- **Data Quality Checks**:
  - Completeness score
  - Uniqueness score
  - Validity score
  - Consistency score
  - Detailed issue reporting

### Star Schema Generation
- Automatic dimension table creation
- Time dimension with full attributes (year, quarter, month, week, day)
- Fact table with measure columns and dimension foreign keys
- DDL script generation for PostgreSQL

### Dashboard
- KPI cards with aggregated measures
- Time series charts
- Bar/line/area/pie visualizations
- Dimension filtering
- English/French localization

### LLM Integration
- Natural language to SQL query generation
- Schema assistance recommendations
- Data quality improvement suggestions
- Powered by local Ollama (LLaMA 3 8B)

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Frontend     │────▶│    Backend      │────▶│   PostgreSQL    │
│  React + Vite   │     │    FastAPI      │     │   Data Warehouse│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │     Ollama      │
                        │   LLaMA 3 8B    │
                        └─────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- (Optional) NVIDIA GPU for LLM acceleration

### Installation

1. **Clone the repository**
   ```bash
   cd bi-platform
   ```

2. **Copy environment file**
   ```bash
   cp .env.example .env
   ```

3. **Start with Docker Compose**
   ```bash
   docker-compose up -d
   ```

4. **Pull LLaMA model (first time only)**
   ```bash
   docker exec -it bi-ollama ollama pull llama3:8b
   ```

5. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/docs

### Local Development

#### Backend

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn main:app --reload --port 8000
```

#### Frontend

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

## API Endpoints

### Datasets
- `POST /api/datasets/upload` - Upload dataset
- `GET /api/datasets` - List datasets
- `GET /api/datasets/{id}/preview` - Preview data
- `DELETE /api/datasets/{id}` - Delete dataset

### ETL
- `POST /api/etl/analyze/{dataset_id}` - Analyze schema
- `POST /api/etl/quality-check/{dataset_id}` - Run quality checks
- `POST /api/etl/run` - Execute ETL pipeline
- `GET /api/etl/status/{job_id}` - Check job status

### Warehouse
- `GET /api/warehouse/tables` - List warehouse tables
- `GET /api/warehouse/schema` - Get star schema
- `POST /api/warehouse/query` - Execute SQL query
- `GET /api/warehouse/dimensions/{dim}/values` - Get dimension values

### Dashboard
- `GET /api/dashboard/kpis` - Get KPI metrics
- `GET /api/dashboard/timeseries` - Time series data
- `POST /api/dashboard/aggregate` - Aggregated data
- `GET /api/dashboard/filters` - Filter options

### LLM
- `GET /api/llm/status` - Check LLM availability
- `POST /api/llm/query` - Generate SQL from natural language
- `POST /api/llm/natural-query` - Query with optional execution

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:postgres@localhost:5432/bi_warehouse` |
| `OLLAMA_BASE_URL` | Ollama API URL | `http://localhost:11434` |
| `OLLAMA_MODEL` | LLM model name | `llama3:8b` |
| `DQ_COMPLETENESS_THRESHOLD` | Minimum completeness score | `0.95` |

## Usage Guide

### 1. Upload Dataset

Navigate to **Data Sources** and upload a CSV/Excel/JSON file.

### 2. Run ETL Pipeline

1. Go to **ETL Pipeline**
2. Select your dataset
3. Configure transformation options
4. Click **Start ETL**

### 3. View Dashboard

After ETL completes, visit **Dashboard** to see:
- KPI metrics
- Time series charts
- Data aggregations

### 4. Query with Natural Language

Go to **Warehouse** and use the natural language query feature:

> "What are the total sales by product category for this year?"

The LLM will generate the appropriate SQL query.

## Localization

The platform supports:
- 🇬🇧 English
- 🇫🇷 Français

Switch languages using the toggle in the header.

## Technology Stack

### Backend
- Python 3.11
- FastAPI
- SQLAlchemy
- Pandas
- PostgreSQL

### Frontend
- React 18
- TypeScript
- Vite
- TailwindCSS
- Recharts
- i18next

### Infrastructure
- Docker
- Docker Compose
- Nginx
- Ollama (LLaMA 3)

## Directory Structure

```
bi-platform/
├── backend/
│   ├── api/routes/          # API endpoints
│   ├── core/                # Database, models, schemas
│   ├── services/            # Business logic
│   ├── utils/               # Utilities
│   └── main.py              # FastAPI entry point
├── frontend/
│   ├── src/
│   │   ├── components/      # React components
│   │   ├── pages/           # Page components
│   │   ├── services/        # API client
│   │   ├── store/           # Redux store
│   │   └── locales/         # i18n translations
│   └── index.html
├── sql/                     # SQL scripts
├── docker-compose.yml
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and feature requests, please open a GitHub issue.

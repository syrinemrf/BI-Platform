# LLM-Powered ETL Automation — Research Suite

Experimental framework for the paper:
**"LLM-Powered Enterprise ETL Automation: Confidence-Gated Routing, Self-Correction, and Human-in-the-Loop Governance"**

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements_research.txt

# 2. (Optional) Configure real LLM access
cp .env.example .env
# Edit .env with your API keys

# 3. Generate synthetic datasets
python generate_datasets.py

# 4. Run notebooks in order (00 → 08)
jupyter notebook notebooks/
```

## Directory Structure

```
research/
├── data/
│   ├── raw/                  # 4 synthetic datasets (CSV, JSON, XML)
│   └── ground_truth/         # Expected star schemas and cleaning rules
├── notebooks/
│   ├── 00_setup_and_datasets.ipynb
│   ├── 01_schema_profiling.ipynb
│   ├── 02_llm_schema_mapping.ipynb
│   ├── 03_data_cleaning.ipynb
│   ├── 04_code_generation_dvr.ipynb
│   ├── 05_hitl_validation.ipynb
│   ├── 06_end_to_end_pipeline.ipynb
│   ├── 07_ablation_study.ipynb
│   └── 08_results_and_figures.ipynb
├── src/                      # Reusable research modules
│   ├── ingestion.py          # Multi-format data loader
│   ├── profiler.py           # Schema profiling & fingerprinting
│   ├── llm_client.py         # LLM client with MockLLMClient fallback
│   ├── schema_mapper.py      # 3-condition schema mapping
│   ├── cleaning_agent.py     # Data cleaning rule detection & application
│   ├── code_generator.py     # ETL code generation with DVR loop
│   ├── hitl_validator.py     # HITL governance layer
│   ├── evaluator.py          # Metrics computation
│   └── visualizer.py         # IEEE-style publication figures
├── results/
│   ├── figures/              # PDF + PNG figures (300 DPI)
│   ├── tables/               # LaTeX tables
│   └── metrics/              # JSON metrics from each notebook
├── paper/
│   └── paper.tex             # IEEE conference paper
├── generate_datasets.py      # Dataset generation script
├── requirements_research.txt
├── .env.example
└── README_research.md        # This file
```

## Datasets

| Dataset | Format | Rows | Difficulty | Key Issues |
|---------|--------|------|------------|------------|
| Retail Sales | CSV | 500 | Easy | Nulls, duplicates, negative prices |
| Hospital Records | JSON | 300 | Medium | Missing fields, date formats, nested data |
| Supplier Invoices | XML | 200 | Hard | XML namespaces, currency formats, missing elements |
| E-commerce Events | JSON | 800 | Medium | Mixed types, timestamps, nested attributes |

All datasets are generated with `random.seed(42)` for reproducibility.

## Key Innovations

1. **Confidence-Gated Routing**: Dynamically selects between local LLaMA and cloud Claude based on schema complexity and confidence threshold (θ=0.75).

2. **DVR Self-Correction**: Divide-Verify-Refine loop for ETL code generation. Python validated via `ast.parse()`, SQL via structural pattern matching. Optimal at K=2 correction attempts.

3. **Adaptive Few-Shot Memory**: Human-approved schema mappings are stored and injected as few-shot examples in subsequent runs.

## Running Without LLMs

All modules include a `MockLLMClient` that generates realistic responses based on schema analysis, enabling full reproducibility without Ollama or API keys. The mock client:
- Analyzes column names and types to produce plausible mappings
- Injects controlled errors based on confidence levels
- Simulates realistic latency distributions

## Compiling the Paper

```bash
cd paper/
pdflatex paper.tex
bibtex paper
pdflatex paper.tex
pdflatex paper.tex
```

## License

Research use only. Part of the BI-Platform project.

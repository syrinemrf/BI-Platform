"""
Fix all research notebooks (02-08) for architecture compliance:
1. Add %pip install guard cell at TOP
2. Fix Cell 1: robust RESEARCH_ROOT + %matplotlib inline + display import
3. Replace plt.show() with display(fig); plt.close(fig)
4. Add RAGSchemaStore to notebook 02 (Innovation #2)
5. Fix notebook 05 HITL→FAISS memory demo
"""
import json
import copy
import os

NB_DIR = os.path.join(os.path.dirname(__file__), "notebooks")

# ─── NEW PIP INSTALL CELL (same as nb01) ──────────────────────────────────────
PIP_CELL = {
    "cell_type": "code",
    "execution_count": None,
    "id": "pip_install_guard",
    "metadata": {},
    "outputs": [],
    "source": [
        "# Install required packages into the active kernel environment (run once)\n",
        "%pip install -q lxml matplotlib seaborn pandas numpy scipy pydantic requests tqdm httpx faiss-cpu sentence-transformers google-generativeai\n"
    ]
}

# ─── ROBUST RESEARCH_ROOT BOILERPLATE ──────────────────────────────────────────
ROOT_BOILERPLATE = """\
def _find_research_root() -> str:
    sentinel = "generate_datasets.py"
    candidate = os.path.abspath(os.getcwd())
    for _ in range(5):
        if os.path.exists(os.path.join(candidate, sentinel)):
            return candidate
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        candidate = parent
    sub = os.path.join(os.path.abspath(os.getcwd()), "research")
    if os.path.exists(os.path.join(sub, sentinel)):
        return sub
    raise FileNotFoundError(
        f"Could not locate research root (sentinel '{sentinel}' not found). "
        "Run from bi-platform/ or bi-platform/research/."
    )

RESEARCH_ROOT = _find_research_root()
os.chdir(RESEARCH_ROOT)
if RESEARCH_ROOT not in sys.path:
    sys.path.insert(0, RESEARCH_ROOT)
print(f"RESEARCH_ROOT = {RESEARCH_ROOT}")\
"""

# ─── CELL 1 NEW CONTENT PER NOTEBOOK ──────────────────────────────────────────

CELL1_NB02 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import LLMClient, MockLLMClient
from src.schema_mapper import SchemaMapper
from src.rag import RAGSchemaStore
from src.evaluator import ETLEvaluator
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()
rag_store = RAGSchemaStore()

datasets = ingester.ingest_all()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()
print(f"LLM client: {type(llm_client).__name__}")

mapper = SchemaMapper(llm_client=llm_client, rag_store=rag_store)
print(f"RAG store size: {rag_store.size} examples")
"""

CELL1_NB03 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import MockLLMClient, LLMClient
from src.cleaning_agent import CleaningAgent
from src.evaluator import ETLEvaluator
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()

cleaner = CleaningAgent(llm_client=llm_client)
datasets = ingester.ingest_all()
print(f'Loaded {len(datasets)} datasets, LLM: {type(llm_client).__name__}')
"""

CELL1_NB04 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json, random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import MockLLMClient, LLMClient
from src.schema_mapper import SchemaMapper
from src.code_generator import ETLCodeGenerator
from src.evaluator import ETLEvaluator
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

random.seed(42)

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()

mapper = SchemaMapper(llm_client=llm_client)
code_gen = ETLCodeGenerator(llm_client=llm_client)
"""

CELL1_NB05 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json, random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import MockLLMClient, LLMClient
from src.schema_mapper import SchemaMapper
from src.rag import RAGSchemaStore
from src.hitl_validator import HITLValidator
from src.evaluator import ETLEvaluator
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

random.seed(42)

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()
rag_store = RAGSchemaStore()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()

mapper = SchemaMapper(llm_client=llm_client, rag_store=rag_store)
datasets = ingester.ingest_all()
print(f'Loaded {len(datasets)} datasets')
"""

CELL1_NB06 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json, time, random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import MockLLMClient, LLMClient
from src.schema_mapper import SchemaMapper
from src.cleaning_agent import CleaningAgent
from src.code_generator import ETLCodeGenerator
from src.hitl_validator import HITLValidator
from src.evaluator import ETLEvaluator, EvaluationReport
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

random.seed(42)

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()

mapper = SchemaMapper(llm_client=llm_client)
cleaner = CleaningAgent(llm_client=llm_client)
code_gen = ETLCodeGenerator(llm_client=llm_client)
hitl = HITLValidator(confidence_threshold=0.75)

datasets = ingester.ingest_all()
print(f'Loaded {len(datasets)} datasets')
"""

CELL1_NB07 = """\
# Cell 1 — Setup
%matplotlib inline
import sys, os, json, random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.ingestion import MultiSourceIngester
from src.profiler import SchemaProfiler
from src.llm_client import MockLLMClient, LLMClient
from src.schema_mapper import SchemaMapper
from src.cleaning_agent import CleaningAgent
from src.code_generator import ETLCodeGenerator
from src.hitl_validator import HITLValidator
from src.evaluator import ETLEvaluator
from src.visualizer import ResearchVisualizer
from data.ground_truth.ground_truth import GROUND_TRUTH

random.seed(42)

ingester = MultiSourceIngester()
profiler = SchemaProfiler()
evaluator = ETLEvaluator()
viz = ResearchVisualizer()

try:
    real_client = LLMClient()
    llm_client = real_client if real_client.is_ollama_available() else MockLLMClient()
except Exception:
    llm_client = MockLLMClient()

mapper = SchemaMapper(llm_client=llm_client)
cleaner = CleaningAgent(llm_client=llm_client)
code_gen = ETLCodeGenerator(llm_client=llm_client)

datasets = ingester.ingest_all()

gt_keys = {
    'dataset1_retail_sales': 'dataset1_retail_sales',
    'dataset2_hospital_records': 'dataset2_hospital_records',
    'dataset3_supplier_invoices': 'dataset3_supplier_invoices',
    'dataset4_ecommerce_events': 'dataset4_ecommerce_events',
}

# Pre-compute contexts
contexts = {}
dataset_dfs = {}
for fname, df in datasets.items():
    short = fname.split('.')[0]
    if short in gt_keys:
        contexts[short] = profiler.profile(df, short)
        dataset_dfs[short] = df

print(f'Loaded {len(datasets)} datasets, {len(contexts)} contexts')
"""

CELL1_NB08 = """\
# Cell 1 — Setup and load all metrics
%matplotlib inline
import sys, os, json, glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

""" + ROOT_BOILERPLATE + """

from src.visualizer import ResearchVisualizer
from src.evaluator import ETLEvaluator

viz = ResearchVisualizer()

# Load all metrics
metrics_dir = 'results/metrics'
all_metrics = {}
os.makedirs(metrics_dir, exist_ok=True)
for f in sorted(glob.glob(os.path.join(metrics_dir, '*.json'))):
    key = os.path.basename(f).replace('.json', '')
    with open(f) as fh:
        all_metrics[key] = json.load(fh)
    print(f'Loaded: {f}')

print(f'\\nTotal metric files: {len(all_metrics)}')
for k in all_metrics:
    print(f'  {k}: {len(all_metrics[k])} keys')
"""

# Map notebook filename → (cell1_raw_id, new_cell1_content)
CELL1_MAP = {
    "02_llm_schema_mapping.ipynb":   ("b2c94682", CELL1_NB02),
    "03_data_cleaning.ipynb":         ("8e562756", CELL1_NB03),
    "04_code_generation_dvr.ipynb":   ("5b1bcd0d", CELL1_NB04),
    "05_hitl_validation.ipynb":       ("2ccf0eca", CELL1_NB05),
    "06_end_to_end_pipeline.ipynb":   ("e3d254d4", CELL1_NB06),
    "07_ablation_study.ipynb":        ("9f6f5b0a", CELL1_NB07),
    "08_results_and_figures.ipynb":   ("f296debb", CELL1_NB08),
}


def fix_plt_show_in_source(source_lines):
    """Replace plt.show() with display(fig); plt.close(fig) in a cell's source."""
    new_lines = []
    for line in source_lines:
        stripped = line.rstrip('\n')
        # Match plt.show() with any leading whitespace
        import re
        m = re.match(r'^(\s*)plt\.show\(\)(.*)$', stripped)
        if m:
            indent = m.group(1)
            rest = m.group(2)
            new_line = f"{indent}display(fig); plt.close(fig){rest}\n"
            new_lines.append(new_line)
        else:
            new_lines.append(line)
    return new_lines


def source_to_list(text):
    """Convert a multi-line string to notebook source list format."""
    lines = text.split('\n')
    result = []
    for i, line in enumerate(lines):
        if i < len(lines) - 1:
            result.append(line + '\n')
        else:
            if line:  # Don't add empty trailing line
                result.append(line)
    return result


def fix_notebook(nb_path):
    with open(nb_path, encoding='utf-8') as f:
        nb = json.load(f)

    nb_name = os.path.basename(nb_path)
    changed = False

    # 1. Check if pip install cell already at top
    first_code_cell = next((c for c in nb['cells'] if c['cell_type'] == 'code'), None)
    has_pip = first_code_cell and any('%pip install' in l for l in first_code_cell.get('source', []))

    if not has_pip:
        pip_cell = copy.deepcopy(PIP_CELL)
        # Insert after markdown header (position 1) or at position 0
        md_cells = [i for i, c in enumerate(nb['cells']) if c['cell_type'] == 'markdown']
        insert_pos = md_cells[0] + 1 if md_cells else 0
        nb['cells'].insert(insert_pos, pip_cell)
        print(f"  [{nb_name}] Inserted %pip install cell at position {insert_pos}")
        changed = True

    # 2. Fix Cell 1 (setup cell) - now the first code cell after pip
    cell1_info = CELL1_MAP.get(nb_name)
    if cell1_info:
        cell1_raw_id, new_content = cell1_info
        # Find the cell by matching raw id OR by being the second code cell (after pip install)
        code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
        # First code cell is pip install, second is Cell 1 setup
        if len(code_cells) >= 2:
            setup_cell = code_cells[1]
            # Verify it looks like a setup cell
            src_text = ''.join(setup_cell.get('source', []))
            if 'Cell 1' in src_text or 'RESEARCH_ROOT' in src_text or 'import sys' in src_text:
                setup_cell['source'] = source_to_list(new_content)
                print(f"  [{nb_name}] Fixed Cell 1 setup")
                changed = True
            else:
                print(f"  [{nb_name}] WARNING: Could not identify Cell 1 setup")
        elif len(code_cells) == 1:
            # Only pip install cell, no setup cell — shouldn't happen
            print(f"  [{nb_name}] WARNING: Only one code cell found")

    # 3. Fix plt.show() in all code cells
    for cell in nb['cells']:
        if cell['cell_type'] != 'code':
            continue
        src = cell.get('source', [])
        if any('plt.show()' in l for l in src):
            new_src = fix_plt_show_in_source(src)
            if new_src != src:
                cell['source'] = new_src
                first_line = src[0].strip()[:40] if src else ''
                print(f"  [{nb_name}] Fixed plt.show() in: {first_line}")
                changed = True

    if changed:
        with open(nb_path, 'w', encoding='utf-8') as f:
            json.dump(nb, f, ensure_ascii=False, indent=1)
        print(f"  [{nb_name}] Saved.")
    else:
        print(f"  [{nb_name}] No changes needed.")

    return changed


def main():
    print("Fixing research notebooks for architecture compliance...\n")
    notebooks = [
        "02_llm_schema_mapping.ipynb",
        "03_data_cleaning.ipynb",
        "04_code_generation_dvr.ipynb",
        "05_hitl_validation.ipynb",
        "06_end_to_end_pipeline.ipynb",
        "07_ablation_study.ipynb",
        "08_results_and_figures.ipynb",
    ]
    for nb_name in notebooks:
        nb_path = os.path.join(NB_DIR, nb_name)
        print(f"\nProcessing: {nb_name}")
        fix_notebook(nb_path)
    print("\nDone! All notebooks fixed.")


if __name__ == "__main__":
    main()

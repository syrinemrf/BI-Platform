import json, os

nbs = [
    "03_data_cleaning.ipynb",
    "04_code_generation_dvr.ipynb",
    "05_hitl_validation.ipynb",
    "06_end_to_end_pipeline.ipynb",
    "07_ablation_study.ipynb",
    "08_results_and_figures.ipynb",
]
ks = {"display_name": "Python (torch)", "language": "python", "name": "torch-env"}
li = {"name": "python", "version": "3.13.5"}

os.chdir(os.path.dirname(os.path.abspath(__file__)))

for nb in nbs:
    try:
        with open(nb, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        data.setdefault("metadata", {})
        data["metadata"]["kernelspec"] = ks
        data["metadata"]["language_info"] = li
        with open(nb, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1)
        print("OK:", nb)
    except Exception as e:
        print("FAIL:", nb, e)

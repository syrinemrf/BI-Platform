"""
Publication-ready visualization module.
IEEE-style figures at 300 DPI with serif fonts.
"""
import json
import os
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from .evaluator import EvaluationReport


class ResearchVisualizer:
    """Generate publication-ready figures for the research paper."""

    COLORS = ["#2E4057", "#048A81", "#54C6EB", "#EF6F6C"]
    DIFFICULTY_COLORS = {
        "easy": "#4CAF50",
        "medium": "#FFC107",
        "medium_hard": "#FF9800",
        "hard": "#F44336",
    }

    def __init__(self, output_dir: str = None):
        if output_dir is None:
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "results", "figures",
            )
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.setup_style()

    def setup_style(self):
        """Configure matplotlib for IEEE-style figures."""
        plt.rcParams.update({
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "font.family": "serif",
            "font.size": 10,
            "axes.linewidth": 0.8,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "figure.figsize": (6, 4),
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.1,
        })
        sns.set_palette(self.COLORS)

    def _save(self, fig, name: str):
        """Save figure as PDF and PNG."""
        for ext in ["pdf", "png"]:
            path = os.path.join(self.output_dir, f"{name}.{ext}")
            fig.savefig(path, format=ext, dpi=300, bbox_inches="tight")
        plt.close(fig)

    # ── Figure 0: DQ Baseline ──────────────────────────────
    def plot_dq_baseline(self, datasets: dict) -> plt.Figure:
        """Plot null percentage per column for each dataset (2x2 grid)."""
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        axes = axes.flatten()

        for idx, (name, null_pcts) in enumerate(datasets.items()):
            if idx >= 4:
                break
            ax = axes[idx]
            cols = list(null_pcts.keys())[:15]  # Top 15 columns
            vals = [null_pcts[c] for c in cols]
            colors = [self.COLORS[1] if v > 0.05 else self.COLORS[0] for v in vals]
            ax.barh(cols, vals, color=colors)
            ax.set_xlabel("Null Percentage")
            ax.set_title(name.replace("_", " ").title(), fontsize=10)
            ax.set_xlim(0, max(vals) * 1.2 if vals else 0.2)

        fig.suptitle("Data Quality Baseline — Null Percentage per Column", fontsize=12)
        fig.tight_layout()
        self._save(fig, "fig0_dq_baseline")
        return fig

    # ── Figure 1: Mapping Accuracy ─────────────────────────
    def plot_mapping_accuracy_by_dataset(
        self, data: dict, error_bars: Optional[dict] = None
    ) -> plt.Figure:
        """Bar chart of mapping accuracy per dataset, colored by difficulty."""
        fig, ax = plt.subplots(figsize=(8, 5))

        names = list(data.keys())
        accuracies = [data[n]["accuracy"] for n in names]
        difficulties = [data[n].get("difficulty", "medium") for n in names]
        colors = [self.DIFFICULTY_COLORS.get(d, "#999") for d in difficulties]

        bars = ax.bar(range(len(names)), accuracies, color=colors, edgecolor="black", linewidth=0.5)

        if error_bars:
            yerr = [error_bars.get(n, 0) for n in names]
            ax.errorbar(range(len(names)), accuracies, yerr=yerr,
                        fmt="none", color="black", capsize=3)

        ax.set_xticks(range(len(names)))
        ax.set_xticklabels([n.replace("_", "\n") for n in names], fontsize=8)
        ax.set_ylabel("Mapping Accuracy")
        ax.set_ylim(0, 1.1)
        ax.set_title("Schema Mapping Accuracy by Dataset")

        # Legend for difficulty
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor=c, label=d.replace("_", " ").title())
                           for d, c in self.DIFFICULTY_COLORS.items()]
        ax.legend(handles=legend_elements, loc="upper right", title="Difficulty")

        for bar, acc in zip(bars, accuracies):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                    f"{acc:.2f}", ha="center", va="bottom", fontsize=8)

        fig.tight_layout()
        self._save(fig, "fig1_mapping_accuracy")
        return fig

    # ── Figure 2: Routing Distribution ─────────────────────
    def plot_routing_distribution(self, data: dict) -> plt.Figure:
        """Stacked bar chart of LLM routing per dataset."""
        fig, ax = plt.subplots(figsize=(8, 5))

        names = list(data.keys())
        llama_only = [data[n].get("llama_only", 0) for n in names]
        llama_fallback = [data[n].get("llama_fallback", 0) for n in names]
        claude_only = [data[n].get("claude_only", 0) for n in names]

        x = range(len(names))
        ax.bar(x, llama_only, label="LLaMA Only", color=self.COLORS[0])
        ax.bar(x, llama_fallback, bottom=llama_only,
               label="LLaMA + Fallback", color=self.COLORS[1])
        ax.bar(x, claude_only,
               bottom=[a + b for a, b in zip(llama_only, llama_fallback)],
               label="Claude Only", color=self.COLORS[2])

        ax.set_xticks(x)
        ax.set_xticklabels([n.replace("_", "\n") for n in names], fontsize=8)
        ax.set_ylabel("Number of Calls")
        ax.set_title("LLM Routing Distribution by Dataset")
        ax.legend()

        fig.tight_layout()
        self._save(fig, "fig2_routing_distribution")
        return fig

    # ── Figure 3: Confidence Calibration ───────────────────
    def plot_confidence_vs_accuracy(
        self, confidence_scores: list, accuracy_scores: list
    ) -> plt.Figure:
        """Scatter plot with regression line for confidence calibration."""
        fig, ax = plt.subplots(figsize=(6, 5))

        ax.scatter(confidence_scores, accuracy_scores,
                   alpha=0.6, color=self.COLORS[0], edgecolors="black",
                   linewidth=0.5, s=50)

        # Regression line
        if len(confidence_scores) > 1:
            z = np.polyfit(confidence_scores, accuracy_scores, 1)
            p = np.poly1d(z)
            x_line = np.linspace(min(confidence_scores), max(confidence_scores), 100)
            ax.plot(x_line, p(x_line), "--", color=self.COLORS[3], linewidth=1.5,
                    label=f"Regression (slope={z[0]:.2f})")

            # Pearson correlation
            r = np.corrcoef(confidence_scores, accuracy_scores)[0, 1]
            ax.text(0.05, 0.95, f"Pearson r = {r:.3f}",
                    transform=ax.transAxes, fontsize=9,
                    verticalalignment="top",
                    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))

        # Diagonal (perfect calibration)
        ax.plot([0, 1], [0, 1], ":", color="gray", alpha=0.5, label="Perfect calibration")

        ax.set_xlabel("LLM Confidence Score")
        ax.set_ylabel("Actual Mapping Accuracy")
        ax.set_title("Confidence Calibration")
        ax.set_xlim(0, 1.05)
        ax.set_ylim(0, 1.05)
        ax.legend(loc="lower right")

        fig.tight_layout()
        self._save(fig, "fig3_confidence_calibration")
        return fig

    # ── Figure 4: DQ Improvement ───────────────────────────
    def plot_dq_improvement(
        self, before_scores: list, after_scores: list, dataset_names: list
    ) -> plt.Figure:
        """Grouped bar chart: before vs after cleaning."""
        fig, ax = plt.subplots(figsize=(8, 5))

        x = np.arange(len(dataset_names))
        width = 0.35

        bars1 = ax.bar(x - width / 2, before_scores, width,
                        label="Before Cleaning", color=self.COLORS[3], alpha=0.8)
        bars2 = ax.bar(x + width / 2, after_scores, width,
                        label="After Cleaning", color=self.COLORS[1], alpha=0.8)

        ax.set_xticks(x)
        ax.set_xticklabels([n.replace("_", "\n") for n in dataset_names], fontsize=8)
        ax.set_ylabel("Data Quality Score")
        ax.set_title("Data Quality Improvement: Before vs After Cleaning")
        ax.legend()
        ax.set_ylim(0, 1.1)

        # Add improvement percentages
        for i, (b, a) in enumerate(zip(before_scores, after_scores)):
            improvement = ((a / b) - 1) * 100 if b > 0 else 0
            ax.annotate(f"+{improvement:.1f}%",
                        xy=(i, max(a, b) + 0.03),
                        ha="center", fontsize=8, color=self.COLORS[1],
                        fontweight="bold")

        fig.tight_layout()
        self._save(fig, "fig4_dq_improvement")
        return fig

    # ── Figure 5: Ablation — Few-shot ──────────────────────
    def plot_ablation_fewshot(
        self, k_values: list, accuracy_values: dict
    ) -> plt.Figure:
        """Line chart: mapping accuracy vs number of few-shot examples."""
        fig, ax = plt.subplots(figsize=(7, 5))

        for idx, (dataset, accs) in enumerate(accuracy_values.items()):
            color = self.COLORS[idx % len(self.COLORS)]
            ax.plot(k_values, accs, "o-", color=color, label=dataset,
                    linewidth=1.5, markersize=6)

        ax.set_xlabel("Number of Few-Shot Examples (k)")
        ax.set_ylabel("Mapping Accuracy")
        ax.set_title("Ablation: Effect of Few-Shot Examples on Accuracy")
        ax.set_xticks(k_values)
        ax.legend()
        ax.set_ylim(0, 1.1)
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        self._save(fig, "fig5_ablation_fewshot")
        return fig

    # ── Figure 6: Ablation — Correction attempts ──────────
    def plot_ablation_correction(
        self, attempt_values: list, validity_values: dict
    ) -> plt.Figure:
        """Line chart: SQL validity vs max correction attempts."""
        fig, ax = plt.subplots(figsize=(7, 5))

        for idx, (dataset, vals) in enumerate(validity_values.items()):
            color = self.COLORS[idx % len(self.COLORS)]
            ax.plot(attempt_values, vals, "s-", color=color, label=dataset,
                    linewidth=1.5, markersize=6)

        ax.set_xlabel("Max Correction Attempts")
        ax.set_ylabel("SQL Validity Rate")
        ax.set_title("DVR Self-Correction: Validity Rate vs Attempts")
        ax.set_xticks(attempt_values)
        ax.legend()
        ax.set_ylim(0, 1.1)
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        self._save(fig, "fig6_ablation_correction")
        return fig

    # ── Figure 7: Latency Comparison ───────────────────────
    def plot_latency_comparison(self, data: dict) -> plt.Figure:
        """Box plot: LLaMA vs Claude latency distribution."""
        fig, ax = plt.subplots(figsize=(6, 5))

        llama_lat = data.get("llama", [])
        claude_lat = data.get("claude", [])

        bp = ax.boxplot(
            [llama_lat, claude_lat],
            labels=["LLaMA 3 8B\n(Local)", "Claude 3.5\n(API)"],
            patch_artist=True,
            boxprops=dict(linewidth=0.8),
            medianprops=dict(color="black", linewidth=1.5),
        )

        bp["boxes"][0].set_facecolor(self.COLORS[0])
        bp["boxes"][1].set_facecolor(self.COLORS[2])

        ax.set_ylabel("Latency (ms)")
        ax.set_title("LLM Response Latency Distribution")

        # Add mean markers
        for i, lat in enumerate([llama_lat, claude_lat]):
            if lat:
                mean_val = np.mean(lat)
                ax.scatter(i + 1, mean_val, marker="D", color=self.COLORS[3],
                           s=40, zorder=3, label=f"Mean: {mean_val:.0f}ms" if i == 0 else f"Mean: {mean_val:.0f}ms")
                ax.annotate(f"{mean_val:.0f}ms",
                            xy=(i + 1.15, mean_val), fontsize=8)

        fig.tight_layout()
        self._save(fig, "fig7_latency")
        return fig

    # ── LaTeX Table Generation ─────────────────────────────
    def generate_latex_table(
        self, report: EvaluationReport, output_dir: str = None
    ) -> str:
        """Generate LaTeX table with booktabs formatting."""
        if output_dir is None:
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "results", "tables",
            )
        os.makedirs(output_dir, exist_ok=True)

        lines = [
            r"\begin{table}[htbp]",
            r"\centering",
            r"\caption{Main Results: End-to-End Pipeline Evaluation}",
            r"\label{tab:main_results}",
            r"\begin{tabular}{lccccc}",
            r"\toprule",
            r"Dataset & Mapping Acc. & Cleaning Recall & DQ Improv. & Model & HITL \\",
            r"\midrule",
        ]

        for ds_name, metrics in report.per_dataset.items():
            short_name = ds_name.replace("_", r"\_")
            ma = metrics.get("mapping_accuracy", 0)
            cr = metrics.get("cleaning_recall", 0)
            dq = metrics.get("dq_improvement", 0)
            model = metrics.get("model_used", "--")
            hitl = metrics.get("hitl_escalated", "--")
            lines.append(
                f"  {short_name} & {ma:.2f} & {cr:.2f} & "
                f"+{dq:.1%} & {model} & {hitl} \\\\"
            )

        lines.extend([
            r"\midrule",
            f"  \\textbf{{Average}} & \\textbf{{{report.overall_mapping_accuracy:.2f}}} & "
            f"\\textbf{{{report.overall_cleaning_recall:.2f}}} & "
            f"\\textbf{{+{report.overall_dq_improvement:.1%}}} & -- & "
            f"{report.hitl_escalation_rate:.0%} \\\\",
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ])

        table_str = "\n".join(lines)
        path = os.path.join(output_dir, "table_main_results.tex")
        with open(path, "w", encoding="utf-8") as f:
            f.write(table_str)

        return table_str

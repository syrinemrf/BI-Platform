"""Generate sample.xlsx fixture with Sales and Returns sheets."""
import pandas as pd
from pathlib import Path

fixtures = Path(__file__).parent / "fixtures"

sales = pd.DataFrame({
    "date": pd.date_range("2024-01-01", periods=20, freq="D").strftime("%Y-%m-%d"),
    "product": (["Widget A", "Widget B", "Widget C", "Widget A", "Widget B"] * 4),
    "quantity": [10, 5, 8, 12, 3, 15, 7, 9, 11, 6, 14, 2, 18, 4, 13, 1, 16, 20, 8, 10],
    "price": ([19.99, 29.99, 9.99, 19.99, 29.99] * 4),
    "region": (["North", "South", "East", "West"] * 5),
})

returns = pd.DataFrame({
    "date": pd.date_range("2024-01-05", periods=10, freq="3D").strftime("%Y-%m-%d"),
    "product": (["Widget A", "Widget B", "Widget C", "Widget A", "Widget B"] * 2),
    "quantity": [1, 2, 1, 3, 1, 2, 1, 1, 2, 1],
    "reason": (["Defective", "Wrong item", "Changed mind", "Defective", "Wrong item"] * 2),
})

out = fixtures / "sample.xlsx"
with pd.ExcelWriter(out, engine="openpyxl") as writer:
    sales.to_excel(writer, sheet_name="Sales", index=False)
    returns.to_excel(writer, sheet_name="Returns", index=False)

print(f"Created {out}")

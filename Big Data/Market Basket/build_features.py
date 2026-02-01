import pandas as pd
from pathlib import Path

RAW = Path("data/raw/canastas.csv")
SILVER = Path("data/silver")
GOLD = Path("data/gold")

SILVER.mkdir(parents=True, exist_ok=True)
GOLD.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(RAW)

# Clean
df = df.dropna()
df["item"] = df["item"].str.lower().str.strip()

df.to_csv(SILVER/"transactions_clean.csv", index=False)

# Simple aggregations
freq = df.groupby("item").size().reset_index(name="purchase_count")
freq.sort_values("purchase_count", ascending=False).to_csv(GOLD/"top_items.csv", index=False)

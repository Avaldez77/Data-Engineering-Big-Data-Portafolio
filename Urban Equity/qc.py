from pathlib import Path
import pandas as pd

SILVER=Path("data/silver")

def assert_unique(df, key):
    if df[key].isna().any(): raise SystemExit(f"❌ {key} has NULLs")
    if df[key].duplicated().any(): raise SystemExit(f"❌ {key} has duplicates")

def main():
    dim=pd.read_csv(SILVER/"dim_communes.csv"); assert_unique(dim,"commune_id")
    parks=pd.read_csv(SILVER/"fact_green_space.csv"); assert_unique(parks,"commune_id")
    if (parks["green_m2_per_capita"]<0).any() or (parks["green_m2_per_capita"]>10).any():
        raise SystemExit("❌ green_m2_per_capita outside [0,10]")
    budget=pd.read_csv(SILVER/"fact_budget_allocations.csv")
    if budget["budget_per_capita_clp"].isna().any(): raise SystemExit("❌ budget_per_capita_clp has NULLs")
    print("✅ Data quality checks passed.")
if __name__=="__main__": main()

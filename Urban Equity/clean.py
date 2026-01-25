from pathlib import Path
import pandas as pd
from src.utils.cleaning import normalize_region, parse_int_maybe, parse_float_maybe, clamp

BRONZE=Path("data/bronze")
SILVER=Path("data/silver")

def main():
    SILVER.mkdir(parents=True, exist_ok=True)
    communes=pd.read_csv(BRONZE/"communes.csv", dtype=str).drop_duplicates(subset=["commune_id"])
    parks=pd.read_csv(BRONZE/"parks_green_space.csv", dtype=str).drop_duplicates(subset=["commune_id"])
    budgets=pd.read_csv(BRONZE/"budget_allocations.csv", dtype=str).drop_duplicates(subset=["commune_id","fiscal_year"])

    communes["region"]=communes["region"].apply(normalize_region)
    communes["population"]=communes["population"].apply(parse_int_maybe)
    communes["population_density_km2"]=communes["population_density_km2"].apply(parse_float_maybe)

    parks["green_m2_per_capita"]=parks["green_m2_per_capita"].apply(parse_float_maybe)
    parks["parks_count"]=parks["parks_count"].apply(parse_int_maybe)

    budgets["budget_per_capita_clp"]=budgets["budget_per_capita_clp"].apply(parse_int_maybe)

    parks["green_m2_per_capita"]=clamp(parks["green_m2_per_capita"].astype(float), 0.0, 10.0)

    communes["population"]=communes["population"].fillna(communes["population"].median())
    communes["population_density_km2"]=communes["population_density_km2"].fillna(communes["population_density_km2"].median())
    parks["green_m2_per_capita"]=parks["green_m2_per_capita"].fillna(parks["green_m2_per_capita"].median())
    budgets["budget_per_capita_clp"]=budgets["budget_per_capita_clp"].fillna(int(budgets["budget_per_capita_clp"].dropna().median()))

    communes.to_csv(SILVER/"dim_communes.csv", index=False)
    parks.to_csv(SILVER/"fact_green_space.csv", index=False)
    budgets.to_csv(SILVER/"fact_budget_allocations.csv", index=False)

    wide = communes.merge(parks, on="commune_id", how="left").merge(budgets, on="commune_id", how="left")
    wide.to_csv(SILVER/"silver_wide_commune_profile.csv", index=False)
    print("✅ Silver built.")
if __name__=="__main__": main()

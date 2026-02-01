from pathlib import Path
import pandas as pd

SILVER=Path("data/silver")
GOLD=Path("data/gold")

def main():
    GOLD.mkdir(parents=True, exist_ok=True)
    wide=pd.read_csv(SILVER/"silver_wide_commune_profile.csv")

    wide["equity_gap_proxy"]=(
        (wide["population_density_km2"]/wide["population_density_km2"].max())*0.5
        + (1-(wide["green_m2_per_capita"]/wide["green_m2_per_capita"].max()))*0.3
        + (1-(wide["budget_per_capita_clp"]/wide["budget_per_capita_clp"].max()))*0.2
    )

    region=wide.groupby("region", as_index=False).agg(
        total_population=("population","sum"),
        avg_density=("population_density_km2","mean"),
        avg_budget_per_capita=("budget_per_capita_clp","mean"),
        avg_green_m2_per_capita=("green_m2_per_capita","mean"),
        avg_equity_gap_proxy=("equity_gap_proxy","mean"),
        communes=("commune_id","count"),
    )
    region.to_csv(GOLD/"gold_region_summary.csv", index=False)

    top10=wide.sort_values("equity_gap_proxy", ascending=False).head(10)[[
        "commune_id","commune_name","region","population","population_density_km2",
        "budget_per_capita_clp","green_m2_per_capita","equity_gap_proxy"
    ]]
    top10.to_csv(GOLD/"gold_top10_communes.csv", index=False)
    print("✅ Gold published.")
if __name__=="__main__": main()

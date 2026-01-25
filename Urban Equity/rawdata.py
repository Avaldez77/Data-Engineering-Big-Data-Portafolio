from __future__ import annotations
from pathlib import Path
import random, csv

OUT = Path("data/raw")
random.seed(12)

REGIONS = ["Metropolitana","Valparaíso","Biobío","O'Higgins","Coquimbo","Los Lagos","Antofagasta","Maule","La Araucanía","Atacama"]

def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)

def main():
    communes=[]
    for i in range(1,61):
        region=random.choice(REGIONS)
        region_dirty = region if random.random()>0.2 else region.lower()
        name=f"Comuna {chr(64 + (i % 26 or 26))}{i:02d}"
        pop=random.randint(30_000,800_000)
        dens=round(random.uniform(150,9000),1)
        communes.append([f"COM{i:03d}",name,region_dirty, pop if random.random()>0.04 else "", dens if random.random()>0.04 else ""])
    parks=[]
    for c in communes:
        cid=c[0]
        m2=round(random.uniform(0.5,6.0),2)
        if random.random()<0.03: m2=round(random.uniform(10.0,20.0),2)
        parks.append([cid, m2 if random.random()>0.05 else "", random.randint(1,120)])
    budgets=[]
    for c in communes:
        cid=c[0]
        b=random.randint(90_000,420_000)
        budgets.append([cid, str(b) if random.random()>0.06 else f"{b:,}", random.choice(["FY24","FY25"])])
    write_csv(OUT/"communes.csv", ["commune_id","commune_name","region","population","population_density_km2"], communes)
    write_csv(OUT/"parks_green_space.csv", ["commune_id","green_m2_per_capita","parks_count"], parks)
    write_csv(OUT/"budget_allocations.csv", ["commune_id","budget_per_capita_clp","fiscal_year"], budgets)
    print("✅ Raw sample generated in data/raw/")
if __name__=="__main__": main()

"""Load raw CSVs into Bronze.

Bronze = raw data copied as-is into /data/bronze.
"""

from pathlib import Path
import shutil

RAW = Path("data/raw")
BRONZE = Path("data/bronze")

def main():
    BRONZE.mkdir(parents=True, exist_ok=True)
    for fp in RAW.glob("*.csv"):
        shutil.copy(fp, BRONZE / fp.name)
    print("✅ Bronze loaded:", ", ".join(sorted([p.name for p in BRONZE.glob("*.csv")])))

if __name__ == "__main__":
    main()

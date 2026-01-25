from pathlib import Path
import shutil, json
import pandas as pd

RAW=Path("data/raw")
BRONZE=Path("data/bronze")

def main():
    BRONZE.mkdir(parents=True, exist_ok=True)
    schema={}
    for fp in RAW.glob("*.csv"):
        shutil.copy(fp, BRONZE/fp.name)
        df=pd.read_csv(fp, dtype=str)
        schema[fp.name]={"columns": list(df.columns), "row_count": int(df.shape[0])}
    (BRONZE/"_schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    print("✅ Bronze ingested.")
if __name__=="__main__": main()

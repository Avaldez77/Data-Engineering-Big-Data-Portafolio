\
from pyspark.sql import SparkSession
from src.utils.dq import dq_summary

def main():
    spark = SparkSession.builder.appName("observability-lakehouse-dq").getOrCreate()
    df = spark.table("silver_ops_event")
    report = dq_summary(df)
    report.show(truncate=False)

if __name__ == "__main__":
    main()

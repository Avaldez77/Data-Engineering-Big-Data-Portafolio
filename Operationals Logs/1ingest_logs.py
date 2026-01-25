\
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, udf, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.utils.parsing import parse_line

def main():
    spark = SparkSession.builder.appName("observability-lakehouse").getOrCreate()

    parsed_schema = StructType([
        StructField("operation_id", IntegerType(), True),
        StructField("layer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("http_code", IntegerType(), True),
        StructField("error_type", StringType(), True),
        StructField("raw_line", StringType(), True),
    ])

    parse_udf = udf(lambda s: parse_line(s), parsed_schema)

    raw = (
        spark.read.text("data/raw/logs.txt")
        .withColumnRenamed("value", "raw_line")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    # Bronze
    raw.write.mode("append").format("parquet").saveAsTable("bronze_ops_log")

    # Silver
    silver = (
        raw.select(parse_udf(col("raw_line")).alias("p"), "ingest_ts", "source_file")
           .select("p.*", "ingest_ts", "source_file")
           .withColumn("event_id", sha2(concat_ws("||", col("raw_line"), col("source_file"), col("ingest_ts").cast("string")), 256))
           .withColumnRenamed("ingest_ts", "event_ts")
    )

    silver.write.mode("overwrite").format("parquet").saveAsTable("silver_ops_event")

    print("✅ Wrote tables: bronze_ops_log, silver_ops_event")

if __name__ == "__main__":
    main()

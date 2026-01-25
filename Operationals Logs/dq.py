\
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as fsum, when

def dq_summary(df: DataFrame) -> DataFrame:
    """
    Simple data-quality summary for the Silver event table.
    """
    return df.agg(
        count("*").alias("rows"),
        fsum(when(col("error_type") == "unparsed", 1).otherwise(0)).alias("unparsed_rows"),
        fsum(when(col("operation_id").isNull(), 1).otherwise(0)).alias("null_operation_id_rows"),
        fsum(when(col("status") == "error", 1).otherwise(0)).alias("error_rows"),
        fsum(when(col("status") == "success", 1).otherwise(0)).alias("success_rows"),
    )

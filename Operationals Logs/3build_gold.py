\
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("observability-lakehouse-gold").getOrCreate()

    # Create views/tables using Spark SQL to keep the demo runnable.
    spark.sql("""
    CREATE OR REPLACE TABLE gold_kpi_daily AS
    SELECT
      CAST(event_ts AS DATE) AS day,
      layer,
      COUNT(*) AS total_events,
      SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_events,
      (SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*)) AS error_rate,
      1 - (SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*)) AS availability
    FROM silver_ops_event
    WHERE layer IN ('web','db')
    GROUP BY CAST(event_ts AS DATE), layer
    """)

    spark.sql("""
    CREATE OR REPLACE TABLE gold_top_failing_ops AS
    SELECT
      layer,
      operation_id,
      COUNT(*) AS total_events,
      SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_events
    FROM silver_ops_event
    WHERE operation_id IS NOT NULL AND layer IN ('web','db')
    GROUP BY layer, operation_id
    ORDER BY error_events DESC, total_events DESC
    """)

    spark.sql("""
    CREATE OR REPLACE TABLE gold_incidents AS
    WITH per_min AS (
      SELECT
        date_trunc('minute', event_ts) AS t_min,
        layer,
        COUNT(*) AS total,
        SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
      FROM silver_ops_event
      WHERE layer IN ('web','db')
      GROUP BY date_trunc('minute', event_ts), layer
    ),
    scored AS (
      SELECT
        *,
        (errors * 1.0 / total) AS err_rate,
        AVG(errors * 1.0 / total) OVER (
          PARTITION BY layer ORDER BY t_min
          ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS baseline_30m
      FROM per_min
    )
    SELECT
      t_min, layer, total, errors, err_rate, baseline_30m,
      CASE WHEN baseline_30m IS NOT NULL AND err_rate >= baseline_30m * 2 AND errors >= 5 THEN 1 ELSE 0 END AS is_incident
    FROM scored
    """)

    print("✅ Wrote tables: gold_kpi_daily, gold_top_failing_ops, gold_incidents")

if __name__ == "__main__":
    main()

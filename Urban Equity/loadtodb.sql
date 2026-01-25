
-- =====================================================
-- PROJECT: Urban Equity Lakehouse (BigQuery)
-- DESCRIPTION:
-- End-to-end analytics project simulating a Databricks-style
-- lakehouse architecture using BigQuery.
-- =====================================================

-- =====================================================
-- BRONZE LAYER | RAW TABLES
-- =====================================================

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_comunas` (
  id_comuna INT64,
  nombre_comuna STRING,
  id_region INT64
);

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_regiones` (
  id_region INT64,
  nombre_region STRING
);

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_densidad` (
  id_comuna INT64,
  densidad INT64
);

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_poblacion` (
  id_comuna INT64,
  poblacion INT64
);

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_presupuesto` (
  id_comuna INT64,
  presupuesto INT64
);

CREATE OR REPLACE TABLE `PROJECT.urban_bronze.raw_areas_verdes` (
  id_comuna INT64,
  metros_plaza FLOAT64,
  metros_parque FLOAT64
);

-- =====================================================
-- SILVER LAYER | DIMENSIONS
-- =====================================================

CREATE OR REPLACE TABLE `PROJECT.urban_silver.dim_region` AS
SELECT id_region, nombre_region
FROM `PROJECT.urban_bronze.raw_regiones`;

CREATE OR REPLACE TABLE `PROJECT.urban_silver.dim_comuna` AS
SELECT
  c.id_comuna,
  c.nombre_comuna,
  c.id_region,
  r.nombre_region
FROM `PROJECT.urban_bronze.raw_comunas` c
LEFT JOIN `PROJECT.urban_silver.dim_region` r
USING (id_region);

-- =====================================================
-- SILVER LAYER | WIDE TABLE
-- =====================================================

CREATE OR REPLACE TABLE `PROJECT.urban_silver.comuna_profile` AS
SELECT
  dc.id_comuna,
  dc.nombre_comuna,
  dc.id_region,
  dc.nombre_region,
  d.densidad,
  p.poblacion,
  pr.presupuesto,
  av.metros_plaza,
  av.metros_parque,
  SAFE_ADD(COALESCE(av.metros_plaza,0), COALESCE(av.metros_parque,0)) AS metros_verdes_total,
  SAFE_DIVIDE(pr.presupuesto, NULLIF(p.poblacion,0)) AS presupuesto_per_capita,
  SAFE_DIVIDE(SAFE_ADD(COALESCE(av.metros_plaza,0), COALESCE(av.metros_parque,0)), NULLIF(p.poblacion,0)) AS m2_verde_per_capita
FROM `PROJECT.urban_silver.dim_comuna` dc
LEFT JOIN `PROJECT.urban_bronze.raw_densidad` d USING (id_comuna)
LEFT JOIN `PROJECT.urban_bronze.raw_poblacion` p USING (id_comuna)
LEFT JOIN `PROJECT.urban_bronze.raw_presupuesto` pr USING (id_comuna)
LEFT JOIN `PROJECT.urban_bronze.raw_areas_verdes` av USING (id_comuna);

-- =====================================================
-- SILVER LAYER | FACT TABLE (SIMULATED)
-- =====================================================

CREATE OR REPLACE TABLE `PROJECT.urban_silver.fact_monthly_spend` AS
WITH months AS (
  SELECT month
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2025-12-01', INTERVAL 1 MONTH)) AS month
),
cats AS (
  SELECT category FROM UNNEST([
    'parks_green','roads_mobility','security','education','health','digital_services'
  ]) AS category
),
base AS (
  SELECT id_comuna, presupuesto, densidad, poblacion
  FROM `PROJECT.urban_silver.comuna_profile`
),
weights AS (
  SELECT
    id_comuna,
    category,
    CASE category
      WHEN 'parks_green' THEN 0.12
      WHEN 'roads_mobility' THEN 0.22
      WHEN 'security' THEN 0.18
      WHEN 'education' THEN 0.18
      WHEN 'health' THEN 0.20
      WHEN 'digital_services' THEN 0.10
    END AS w
  FROM base, cats
)
SELECT
  b.id_comuna,
  m.month,
  w.category,
  SAFE_DIVIDE(b.presupuesto, 12) AS budget_monthly_baseline,
  CAST(ROUND(
    SAFE_DIVIDE(b.presupuesto, 12)
    * w.w
    * (1 + SAFE_DIVIDE(COALESCE(b.densidad,0), 1000))
    * (1 + SAFE_DIVIDE(COALESCE(b.poblacion,0), 500000))
    * (0.85 + (RAND() * 0.40))
  ) AS INT64) AS spend_amount
FROM base b
CROSS JOIN months m
JOIN weights w USING (id_comuna);

-- =====================================================
-- GOLD LAYER | KPI VIEW
-- =====================================================

CREATE OR REPLACE VIEW `PROJECT.urban_gold.v_kpi_comuna` AS
SELECT
  id_comuna,
  nombre_comuna,
  id_region,
  nombre_region,
  densidad,
  poblacion,
  presupuesto,
  presupuesto_per_capita,
  metros_verdes_total,
  m2_verde_per_capita
FROM `PROJECT.urban_silver.comuna_profile`;

-- =====================================================
-- GOLD LAYER | BIGQUERY ML (OPTIONAL)
-- =====================================================

CREATE OR REPLACE MODEL `PROJECT.urban_gold.m_cluster_comunas`
OPTIONS(model_type='kmeans', num_clusters=4, standardize_features=true) AS
SELECT
  densidad,
  presupuesto_per_capita,
  m2_verde_per_capita
FROM `PROJECT.urban_silver.comuna_profile`
WHERE densidad IS NOT NULL
  AND presupuesto_per_capita IS NOT NULL
  AND m2_verde_per_capita IS NOT NULL;

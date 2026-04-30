-- Databricks notebook source

-- =====================================================
-- 1. TABLE OVERVIEW
-- =====================================================

SELECT *
FROM workspace.default.cars
LIMIT 100;



-- =====================================================
-- 2. SALES DATE RANGE
-- =====================================================

SELECT
    MIN(to_timestamp(saledate,'EEE MMM dd yyyy HH:mm:ss')) AS earliest_sale,
    MAX(to_timestamp(saledate,'EEE MMM dd yyyy HH:mm:ss')) AS latest_sale
FROM workspace.default.cars;



-- =====================================================
-- 3. TOTAL ROWS / DISTINCT VIN / DUPLICATES
-- =====================================================

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT vin) AS distinct_vins,
    COUNT(*) - COUNT(DISTINCT vin) AS duplicate_vin_rows
FROM workspace.default.cars;



-- =====================================================
-- 4. DISTINCT COUNTS
-- =====================================================

SELECT
    COUNT(DISTINCT make)  AS total_makes,
    COUNT(DISTINCT model) AS total_models,
    COUNT(DISTINCT seller) AS total_sellers,
    COUNT(DISTINCT state) AS total_states
FROM workspace.default.cars;



-- =====================================================
-- 5. VEHICLE YEAR RANGE
-- =====================================================

SELECT
    MIN(CAST(year AS INT)) AS oldest_model_year,
    MAX(CAST(year AS INT)) AS newest_model_year
FROM workspace.default.cars;



-- =====================================================
-- 6. PRICE / REVENUE / ODOMETER / CONDITION
-- =====================================================

SELECT
    MIN(CAST(sellingprice AS INT)) AS min_price,
    MAX(CAST(sellingprice AS INT)) AS max_price,
    ROUND(AVG(CAST(sellingprice AS INT)),0) AS avg_price,

    MIN(to_timestamp(saledate,'EEE MMM dd yyyy HH:mm:ss')) AS start_selling,
    MAX(to_timestamp(saledate,'EEE MMM dd yyyy HH:mm:ss')) AS stop_selling,

    SUM(CAST(sellingprice AS BIGINT)) AS revenue,

    MIN(CAST(mmr AS INT)) AS min_mmr,
    MAX(CAST(mmr AS INT)) AS max_mmr,
    ROUND(AVG(CAST(mmr AS INT)),0) AS avg_mmr,

    MIN(CAST(odometer AS INT)) AS min_odometer,
    MAX(CAST(odometer AS INT)) AS max_odometer,
    ROUND(AVG(CAST(odometer AS INT)),0) AS avg_odometer,

    MIN(CAST(condition AS INT)) AS min_condition,
    MAX(CAST(condition AS INT)) AS max_condition,
    ROUND(AVG(CAST(condition AS INT)),0) AS avg_condition

FROM workspace.default.cars
WHERE sellingprice IS NOT NULL
AND mmr IS NOT NULL
AND odometer IS NOT NULL
AND condition IS NOT NULL;



-- =====================================================
-- 7. DUPLICATE VIN CHECK
-- =====================================================

SELECT
    vin,
    COUNT(*) AS duplicate_count
FROM workspace.default.cars
GROUP BY vin
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;



-- =====================================================
-- 8. PROFIT METRICS
-- =====================================================

SELECT
    sellingprice,
    mmr,

    CASE
        WHEN CAST(mmr AS DOUBLE) > 0
        THEN ROUND(CAST(sellingprice AS DOUBLE) / CAST(mmr AS DOUBLE),2)
    END AS price_to_mmr_ratio,

    ROUND(CAST(sellingprice AS DOUBLE) - CAST(mmr AS DOUBLE),2) AS profit_vs_mmr,

    CASE
        WHEN CAST(mmr AS DOUBLE) > 0
        THEN ROUND(
            ((CAST(sellingprice AS DOUBLE) - CAST(mmr AS DOUBLE))
            / CAST(mmr AS DOUBLE)) * 100,2)
    END AS profit_margin_pct,

    CASE
        WHEN CAST(sellingprice AS DOUBLE) > 0
        THEN ROUND(
            ((CAST(sellingprice AS DOUBLE) - CAST(mmr AS DOUBLE))
            / CAST(sellingprice AS DOUBLE)) * 100,2)
    END AS gross_margin_pct

FROM workspace.default.cars;



-- =====================================================
-- 9. NULL / BAD RECORD CHECK
-- =====================================================

SELECT *
FROM workspace.default.cars
WHERE vin IS NULL
   OR make IS NULL
   OR model IS NULL
   OR sellingprice IS NULL
   OR mmr IS NULL
   OR odometer IS NULL
   OR saledate IS NULL
   OR year IS NULL
   OR trim IS NULL
   OR body IS NULL
   OR transmission IS NULL
   OR state IS NULL
   OR condition IS NULL
   OR color IS NULL
   OR interior IS NULL
   OR seller IS NULL;



-- =====================================================
-- 10. CLEANED DATASET + FEATURE ENGINEERING
-- =====================================================

WITH carsales AS (

SELECT DISTINCT

    TRIM(vin) AS vin,

    COALESCE(NULLIF(INITCAP(TRIM(make)),''),'Unknown') AS manufacture,
    COALESCE(NULLIF(INITCAP(TRIM(model)),''),'Unknown') AS model,
    COALESCE(NULLIF(INITCAP(TRIM(trim)),''),'Unknown') AS features,
    COALESCE(NULLIF(INITCAP(TRIM(body)),''),'Unknown') AS bodytype,
    COALESCE(NULLIF(INITCAP(TRIM(transmission)),''),'Unknown') AS transmission,
    COALESCE(NULLIF(INITCAP(TRIM(color)),''),'Unknown') AS ext_color,
    COALESCE(NULLIF(INITCAP(TRIM(interior)),''),'Unknown') AS int_color,
    COALESCE(NULLIF(INITCAP(TRIM(seller)),''),'Unknown') AS supplier,

    LOWER(TRIM(state)) AS state_code,

    CAST(year AS INT) AS model_year,
    CAST(condition AS INT) AS condition_score,
    CAST(odometer AS INT) AS mileage,
    CAST(mmr AS INT) AS market_value,
    CAST(sellingprice AS INT) AS selling_price,

    to_timestamp(saledate,'EEE MMM dd yyyy HH:mm:ss') AS sale_date

FROM workspace.default.cars

)

SELECT

    vin,
    manufacture,
    model,
    features,
    bodytype,

    CASE
        WHEN LOWER(bodytype) LIKE '%suv%' THEN 'SUV'
        WHEN LOWER(bodytype) LIKE '%sedan%' THEN 'Sedan'
        WHEN LOWER(bodytype) LIKE '%truck%' THEN 'Truck'
        WHEN LOWER(bodytype) LIKE '%coupe%' THEN 'Coupe'
        WHEN LOWER(bodytype) LIKE '%hatch%' THEN 'Hatchback'
        ELSE 'Other'
    END AS vehicle_segment,

    transmission,
    supplier,

    CASE
        WHEN LOWER(supplier) LIKE '%hertz%'
          OR LOWER(supplier) LIKE '%avis%'
          OR LOWER(supplier) LIKE '%budget%'
          OR LOWER(supplier) LIKE '%enterprise%' THEN 'Rental Fleet'

        WHEN LOWER(supplier) LIKE '%wells fargo%'
          OR LOWER(supplier) LIKE '%chase%'
          OR LOWER(supplier) LIKE '%ally%'
          OR LOWER(supplier) LIKE '%santander%' THEN 'Finance / Lease'

        WHEN LOWER(supplier) LIKE '%toyota%'
          OR LOWER(supplier) LIKE '%ford%'
          OR LOWER(supplier) LIKE '%honda%' THEN 'Manufacturer / OEM'

        WHEN LOWER(supplier) LIKE '%fleet%'
          OR LOWER(supplier) LIKE '%leaseplan%' THEN 'Corporate Fleet'

        WHEN LOWER(supplier) LIKE '%motors%'
          OR LOWER(supplier) LIKE '%auto sales%'
          OR LOWER(supplier) LIKE '%wholesale%' THEN 'Dealer / Wholesale'

        WHEN supplier = 'Unknown' THEN 'Unknown'

        ELSE 'Independent Dealer'
    END AS supplier_type,

    model_year,
    2015 - model_year AS vehicle_age,

    CASE
        WHEN 2015 - model_year <= 1 THEN 'New'
        WHEN 2015 - model_year <= 3 THEN 'Nearly New'
        WHEN 2015 - model_year <= 5 THEN 'Recent'
        WHEN 2015 - model_year <= 10 THEN 'Mid-Age'
        ELSE 'Older'
    END AS age_bucket,

    selling_price,
    market_value,
    mileage,
    condition_score,

    ROUND(selling_price - market_value,2) AS profit_vs_market,
    ROUND(((selling_price - market_value) / market_value) * 100,2) AS profit_margin_pct,
    ROUND(selling_price / market_value,4) AS price_to_mmr_ratio,

    CASE
        WHEN selling_price > market_value THEN 'Strong Retention'
        WHEN selling_price = market_value THEN 'At Market'
        ELSE 'Weak Retention'
    END AS retention,

    CASE
        WHEN mileage < 5000 THEN 'New / Delivery'
        WHEN mileage BETWEEN 5000 AND 20000 THEN 'Very Low'
        WHEN mileage BETWEEN 20001 AND 40000 THEN 'Low'
        WHEN mileage BETWEEN 40001 AND 65000 THEN 'Below Average'
        WHEN mileage BETWEEN 65001 AND 90000 THEN 'Average'
        WHEN mileage BETWEEN 90001 AND 120000 THEN 'Above Average'
        WHEN mileage BETWEEN 120001 AND 160000 THEN 'High'
        ELSE 'Very High'
    END AS mileage_bucket,

    CASE
        WHEN condition_score >= 40 THEN 'Excellent'
        WHEN condition_score >= 30 THEN 'Good'
        WHEN condition_score >= 20 THEN 'Fair'
        WHEN condition_score >= 10 THEN 'Poor'
        ELSE 'Very Poor'
    END AS condition_tier,

    TO_DATE(sale_date) AS sale_date,
    DATE_FORMAT(sale_date,'EEEE') AS day_of_week,
    DATE_FORMAT(sale_date,'HH:mm:ss') AS transaction_time,

    CASE
        WHEN HOUR(sale_date) BETWEEN 0 AND 11 THEN 'Morning'
        WHEN HOUR(sale_date) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN HOUR(sale_date) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_group,

    CASE
        WHEN dayofweek(sale_date) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_category,

    DATE_FORMAT(sale_date,'MMMM') AS month_name,

    CASE
        WHEN DAY(sale_date) BETWEEN 1 AND 10 THEN 'Beginning of Month'
        WHEN DAY(sale_date) BETWEEN 11 AND 20 THEN 'Mid of Month'
        ELSE 'End of Month'
    END AS month_pattern

FROM carsales
GROUP BY ALL;

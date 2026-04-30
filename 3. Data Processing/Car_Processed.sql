-- Databricks notebook source
--Table Overview
SELECT *
FROM `workspace`.`default`.`cars`
LIMIT 100;



-- ==============================================================================================
-- TABLE OVERVIEW
-- ==============================================================================================

SELECT *
FROM `workspace`.`default`.`cars`
LIMIT 100;


-- ==============================================================================================
-- DATA INSPECTION
-- ==============================================================================================


-- Sales Date Range
-- BigQuery: SAFE.PARSE_DATETIME('%a %b %d %Y %H:%M:%S', ...)
-- Databricks: to_timestamp() with pattern; TRIM() still works

SELECT
    MIN(parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss')) AS earliest_sale,
    MAX(parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss')) AS latest_sale
FROM `workspace`.`default`.`cars`;



-- Total Rows / Distinct VINs / Duplicate VINs
SELECT
    COUNT(*)                                   AS total_rows,
    COUNT(DISTINCT vin)                        AS distinct_vins,
    COUNT(*) - COUNT(DISTINCT vin)             AS duplicate_vin_rows
FROM `workspace`.`default`.`cars`;


-- Different Makes / Models / Sellers / States
SELECT
    COUNT(DISTINCT make)    AS total_makes,
    COUNT(DISTINCT model)   AS total_models,
    COUNT(DISTINCT seller)  AS total_sellers,
    COUNT(DISTINCT state)   AS total_states
FROM `workspace`.`default`.`cars`;


-- Vehicle Year Range
-- BigQuery: SAFE_CAST → Databricks: TRY_CAST (returns NULL on failure, no error)
SELECT
    MIN(TRY_CAST(year AS INT))  AS oldest_model_year,
    MAX(TRY_CAST(year AS INT))  AS newest_model_year
FROM `workspace`.`default`.`cars`;


-- Price / Revenue / MMR / Odometer / Condition Overview
SELECT
    MIN(TRY_CAST(sellingprice AS INT))                          AS min_price,
    MAX(TRY_CAST(sellingprice AS INT))                          AS max_price,
    ROUND(AVG(TRY_CAST(sellingprice AS INT)), 0)                AS avg_price,

    MIN(parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss'))  AS start_SELLING,
    MAX(parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss'))  AS stop_SELLING,

    SUM(TRY_CAST(sellingprice AS BIGINT))                       AS Revenue,

    MIN(TRY_CAST(mmr AS INT))                                   AS min_mmr,
    MAX(TRY_CAST(mmr AS INT))                                   AS max_mmr,
    ROUND(AVG(TRY_CAST(mmr AS INT)), 0)                         AS avg_mmr,

    MIN(TRY_CAST(odometer AS INT))                              AS min_odometer,
    MAX(TRY_CAST(odometer AS INT))                              AS max_odometer,
    ROUND(AVG(TRY_CAST(odometer AS INT)), 0)                    AS avg_odometer,

    MIN(TRY_CAST(condition AS INT))                             AS min_condition,
    MAX(TRY_CAST(condition AS INT))                             AS max_condition,
    ROUND(AVG(TRY_CAST(condition AS INT)), 0)                   AS AVG_condition

FROM `workspace`.`default`.`cars`
WHERE sellingprice IS NOT NULL
  AND condition     IS NOT NULL
  AND mmr           IS NOT NULL
  AND odometer      IS NOT NULL;


-- Duplicate VIN Check
SELECT
    vin,
    COUNT(*) AS duplicate_count
FROM `workspace`.`default`.`cars`
GROUP BY vin
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- Profit Metrics
SELECT
    TRY_CAST(sellingprice AS DOUBLE)                                                    AS sellingprice,
    TRY_CAST(mmr AS DOUBLE)                                                             AS mmr,

    CASE
        WHEN TRY_CAST(mmr AS DOUBLE) > 0
        THEN ROUND(TRY_CAST(sellingprice AS DOUBLE) / TRY_CAST(mmr AS DOUBLE), 2)
    END AS price_to_mmr_ratio,

    ROUND(TRY_CAST(sellingprice AS DOUBLE) - TRY_CAST(mmr AS DOUBLE), 2)               AS profit_vs_mmr,

    CASE
        WHEN TRY_CAST(mmr AS DOUBLE) > 0
        THEN ROUND(
                (TRY_CAST(sellingprice AS DOUBLE) - TRY_CAST(mmr AS DOUBLE))
                / TRY_CAST(mmr AS DOUBLE) * 100,
             2)
    END AS profit_margin_pct,

    CASE
        WHEN TRY_CAST(sellingprice AS DOUBLE) > 0
        THEN ROUND(
                (TRY_CAST(sellingprice AS DOUBLE) - TRY_CAST(mmr AS DOUBLE))
                / TRY_CAST(sellingprice AS DOUBLE) * 100,
             2)
    END AS gross_margin_pct

FROM `workspace`.`default`.`cars`;


-- NULL / Bad Record Check
SELECT *
FROM `workspace`.`default`.`cars`
WHERE vin          IS NULL
   OR make         IS NULL
   OR model        IS NULL
   OR sellingprice IS NULL
   OR mmr          IS NULL
   OR odometer     IS NULL
   OR saledate     IS NULL
   OR year         IS NULL
   OR trim         IS NULL
   OR body         IS NULL
   OR transmission IS NULL
   OR state        IS NULL
   OR condition    IS NULL
   OR color        IS NULL
   OR interior     IS NULL
   OR seller       IS NULL;


-- ==============================================================================================
-- DATA CLEANING
-- ==============================================================================================
-- BigQuery: INITCAP() → Databricks: INITCAP() ✓ (supported in Spark SQL)
-- BigQuery: NULLIF(x,'—') chain → same pattern works in Spark SQL
-- BigQuery: COALESCE → same ✓

SELECT
    TRIM(vin)                                                                        AS Vin,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(make)),    ''), '—'), 'Unknown')            AS Manufacture,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(model)),   ''), '—'), 'Unknown')            AS Model,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(trim)),    ''), '—'), 'Unknown')            AS Features,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(body)),    ''), '—'), 'Unknown')            AS BodyType,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(transmission)), ''), '—'), 'Unknown')       AS Transmission,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(color)),   ''), '—'), 'Unknown')            AS Ext_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(interior)),''), '—'), 'Unknown')            AS Int_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(seller)),  ''), '—'), 'Unknown')            AS Supplier,
    LOWER(TRIM(state))                                                              AS state_code,

    CASE
        WHEN LOWER(TRIM(state)) = 'al' THEN 'Alabama'
        WHEN LOWER(TRIM(state)) = 'az' THEN 'Arizona'
        WHEN LOWER(TRIM(state)) = 'ca' THEN 'California'
        WHEN LOWER(TRIM(state)) = 'co' THEN 'Colorado'
        WHEN LOWER(TRIM(state)) = 'fl' THEN 'Florida'
        WHEN LOWER(TRIM(state)) = 'ga' THEN 'Georgia'
        WHEN LOWER(TRIM(state)) = 'hi' THEN 'Hawaii'
        WHEN LOWER(TRIM(state)) = 'il' THEN 'Illinois'
        WHEN LOWER(TRIM(state)) = 'in' THEN 'Indiana'
        WHEN LOWER(TRIM(state)) = 'la' THEN 'Louisiana'
        WHEN LOWER(TRIM(state)) = 'ma' THEN 'Massachusetts'
        WHEN LOWER(TRIM(state)) = 'md' THEN 'Maryland'
        WHEN LOWER(TRIM(state)) = 'mi' THEN 'Michigan'
        WHEN LOWER(TRIM(state)) = 'mn' THEN 'Minnesota'
        WHEN LOWER(TRIM(state)) = 'mo' THEN 'Missouri'
        WHEN LOWER(TRIM(state)) = 'ms' THEN 'Mississippi'
        WHEN LOWER(TRIM(state)) = 'nc' THEN 'North Carolina'
        WHEN LOWER(TRIM(state)) = 'ne' THEN 'Nebraska'
        WHEN LOWER(TRIM(state)) = 'nj' THEN 'New Jersey'
        WHEN LOWER(TRIM(state)) = 'nm' THEN 'New Mexico'
        WHEN LOWER(TRIM(state)) = 'nv' THEN 'Nevada'
        WHEN LOWER(TRIM(state)) = 'ny' THEN 'New York'
        WHEN LOWER(TRIM(state)) = 'oh' THEN 'Ohio'
        WHEN LOWER(TRIM(state)) = 'ok' THEN 'Oklahoma'
        WHEN LOWER(TRIM(state)) = 'or' THEN 'Oregon'
        WHEN LOWER(TRIM(state)) = 'pa' THEN 'Pennsylvania'
        WHEN LOWER(TRIM(state)) = 'pr' THEN 'Puerto Rico'
        WHEN LOWER(TRIM(state)) = 'sc' THEN 'South Carolina'
        WHEN LOWER(TRIM(state)) = 'tn' THEN 'Tennessee'
        WHEN LOWER(TRIM(state)) = 'tx' THEN 'Texas'
        WHEN LOWER(TRIM(state)) = 'ut' THEN 'Utah'
        WHEN LOWER(TRIM(state)) = 'va' THEN 'Virginia'
        WHEN LOWER(TRIM(state)) = 'wa' THEN 'Washington'
        WHEN LOWER(TRIM(state)) = 'wi' THEN 'Wisconsin'
        -- Canada
        WHEN LOWER(TRIM(state)) = 'ab' THEN 'Alberta'
        WHEN LOWER(TRIM(state)) = 'ns' THEN 'Nova Scotia'
        WHEN LOWER(TRIM(state)) = 'on' THEN 'Ontario'
        WHEN LOWER(TRIM(state)) = 'qc' THEN 'Quebec'
        ELSE 'Unknown'
    END AS StateName,

    CASE
        WHEN LOWER(TRIM(state)) IN ('ab','ns','on','qc') THEN 'Canada'
        ELSE 'USA'
    END AS Country,

    TRY_CAST(year      AS INT)    AS Model_Year,
    TRY_CAST(condition AS INT)    AS Condition_Score,
    TRY_CAST(odometer  AS INT)    AS Mileage,
    TRY_CAST(mmr       AS INT)    AS Market_Value,
    TRY_CAST(sellingprice AS INT) AS Selling_Price,

    -- BigQuery: PARSE_DATETIME → Databricks: to_timestamp()
   parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss') AS Sale_Date

FROM `workspace`.`default`.`cars`;



-- ==============================================================================================

WITH carsales AS (

    SELECT DISTINCT
        year,
        TRIM(vin)                                                                           AS Vin,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(make)),        ''), '—'), 'Unknown')           AS Manufacture,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(model)),       ''), '—'), 'Unknown')           AS Model,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(trim)),        ''), '—'), 'Unknown')           AS Features,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(body)),        ''), '—'), 'Unknown')           AS BodyType,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(transmission)),''), '—'), 'Unknown')           AS Transmission,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(color)),       ''), '—'), 'Unknown')           AS Ext_color,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(interior)),    ''), '—'), 'Unknown')           AS Int_color,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(seller)),      ''), '—'), 'Unknown')           AS Supplier,
        LOWER(TRIM(state))                                                                  AS state_code,

        CASE
            WHEN LOWER(TRIM(state)) = 'al' THEN 'Alabama'
            WHEN LOWER(TRIM(state)) = 'az' THEN 'Arizona'
            WHEN LOWER(TRIM(state)) = 'ca' THEN 'California'
            WHEN LOWER(TRIM(state)) = 'co' THEN 'Colorado'
            WHEN LOWER(TRIM(state)) = 'fl' THEN 'Florida'
            WHEN LOWER(TRIM(state)) = 'ga' THEN 'Georgia'
            WHEN LOWER(TRIM(state)) = 'hi' THEN 'Hawaii'
            WHEN LOWER(TRIM(state)) = 'il' THEN 'Illinois'
            WHEN LOWER(TRIM(state)) = 'in' THEN 'Indiana'
            WHEN LOWER(TRIM(state)) = 'la' THEN 'Louisiana'
            WHEN LOWER(TRIM(state)) = 'ma' THEN 'Massachusetts'
            WHEN LOWER(TRIM(state)) = 'md' THEN 'Maryland'
            WHEN LOWER(TRIM(state)) = 'mi' THEN 'Michigan'
            WHEN LOWER(TRIM(state)) = 'mn' THEN 'Minnesota'
            WHEN LOWER(TRIM(state)) = 'mo' THEN 'Missouri'
            WHEN LOWER(TRIM(state)) = 'ms' THEN 'Mississippi'
            WHEN LOWER(TRIM(state)) = 'nc' THEN 'North Carolina'
            WHEN LOWER(TRIM(state)) = 'ne' THEN 'Nebraska'
            WHEN LOWER(TRIM(state)) = 'nj' THEN 'New Jersey'
            WHEN LOWER(TRIM(state)) = 'nm' THEN 'New Mexico'
            WHEN LOWER(TRIM(state)) = 'nv' THEN 'Nevada'
            WHEN LOWER(TRIM(state)) = 'ny' THEN 'New York'
            WHEN LOWER(TRIM(state)) = 'oh' THEN 'Ohio'
            WHEN LOWER(TRIM(state)) = 'ok' THEN 'Oklahoma'
            WHEN LOWER(TRIM(state)) = 'or' THEN 'Oregon'
            WHEN LOWER(TRIM(state)) = 'pa' THEN 'Pennsylvania'
            WHEN LOWER(TRIM(state)) = 'pr' THEN 'Puerto Rico'
            WHEN LOWER(TRIM(state)) = 'sc' THEN 'South Carolina'
            WHEN LOWER(TRIM(state)) = 'tn' THEN 'Tennessee'
            WHEN LOWER(TRIM(state)) = 'tx' THEN 'Texas'
            WHEN LOWER(TRIM(state)) = 'ut' THEN 'Utah'
            WHEN LOWER(TRIM(state)) = 'va' THEN 'Virginia'
            WHEN LOWER(TRIM(state)) = 'wa' THEN 'Washington'
            WHEN LOWER(TRIM(state)) = 'wi' THEN 'Wisconsin'
            -- Canada
            WHEN LOWER(TRIM(state)) = 'ab' THEN 'Alberta'
            WHEN LOWER(TRIM(state)) = 'ns' THEN 'Nova Scotia'
            WHEN LOWER(TRIM(state)) = 'on' THEN 'Ontario'
            WHEN LOWER(TRIM(state)) = 'qc' THEN 'Quebec'
            ELSE 'Unknown'
        END AS StateName,

        TRY_CAST(year         AS INT)    AS Model_Year,
        TRY_CAST(condition    AS INT)    AS Condition_Score,
        TRY_CAST(odometer     AS INT)    AS Mileage,
        TRY_CAST(mmr          AS INT)    AS Market_Value,
        TRY_CAST(sellingprice AS INT)    AS Selling_Price,
       parse_timestamp(TRIM(saledate), 'EEE MMM dd yyyy HH:mm:ss') AS Sale_Date

    FROM `workspace`.`default`.`cars`

)


SELECT
    -- ── Identity ──────────────────────────────────────────────────────────────
    c.Vin,
    c.Manufacture,
    c.Model,
    c.Features,
    c.BodyType,

    CASE
        WHEN LOWER(c.BodyType) LIKE '%suv%'       THEN 'SUV'
        WHEN LOWER(c.BodyType) LIKE '%sedan%'     THEN 'Sedan'
        WHEN LOWER(c.BodyType) LIKE '%truck%'     THEN 'Truck'
        WHEN LOWER(c.BodyType) LIKE '%coupe%'     THEN 'Coupe'
        WHEN LOWER(c.BodyType) LIKE '%hatchback%' THEN 'Hatchback'
        ELSE 'Other'
    END AS vehicle_segment,

    c.Transmission,
    c.state_code,
    c.StateName,

    CASE
        WHEN c.state_code IN ('ab','ns','on','qc') THEN 'Canada'
        ELSE 'USA'
    END AS Country,

    c.Ext_color,
    c.Int_color,
    c.Supplier,

    -- ── Supplier Classification ───────────────────────────────────────────────
   
    CASE
        WHEN LOWER(c.Supplier) LIKE '%hertz%'
          OR LOWER(c.Supplier) LIKE '%avis%'
          OR LOWER(c.Supplier) LIKE '%budget%'
          OR LOWER(c.Supplier) LIKE '%enterprise%'
          OR LOWER(c.Supplier) LIKE '%u-haul%'
          OR LOWER(c.Supplier) LIKE '%dtg operations%'      THEN 'Rental Fleet'

        WHEN LOWER(c.Supplier) LIKE '%wells fargo%'
          OR LOWER(c.Supplier) LIKE '%jpmorgan chase%'
          OR LOWER(c.Supplier) LIKE '%chase%'
          OR LOWER(c.Supplier) LIKE '%santander%'
          OR LOWER(c.Supplier) LIKE '%tdaf%'
          OR LOWER(c.Supplier) LIKE '%world omni%'
          OR LOWER(c.Supplier) LIKE '%ally financial%'
          OR LOWER(c.Supplier) LIKE '%gm financial%'
          OR LOWER(c.Supplier) LIKE '%toyota financial%'
          OR LOWER(c.Supplier) LIKE '%nissan financial%'    THEN 'Finance / Lease'

        WHEN LOWER(c.Supplier) LIKE '%ford%'
          OR LOWER(c.Supplier) LIKE '%toyota%'
          OR LOWER(c.Supplier) LIKE '%honda%'
          OR LOWER(c.Supplier) LIKE '%kia%'
          OR LOWER(c.Supplier) LIKE '%nissan%'
          OR LOWER(c.Supplier) LIKE '%hyundai%'
          OR LOWER(c.Supplier) LIKE '%gm%'
          OR LOWER(c.Supplier) LIKE '%volkswagen%'
          OR LOWER(c.Supplier) LIKE '%mercedes%'            THEN 'Manufacturer / OEM'

        WHEN LOWER(c.Supplier) LIKE '%ari fleet%'
          OR LOWER(c.Supplier) LIKE '%wheels%'
          OR LOWER(c.Supplier) LIKE '%leaseplan%'
          OR LOWER(c.Supplier) LIKE '%element fleet%'
          OR LOWER(c.Supplier) LIKE '%donlen%'              THEN 'Corporate Fleet'

        WHEN LOWER(c.Supplier) LIKE '%auto sales%'
          OR LOWER(c.Supplier) LIKE '%wholesale%'
          OR LOWER(c.Supplier) LIKE '%motors%'
          OR LOWER(c.Supplier) LIKE '%cars llc%'
          OR LOWER(c.Supplier) LIKE '%auto inc%'            THEN 'Dealer / Wholesale'

        WHEN c.Supplier = 'Unknown'                         THEN 'Unknown'
        ELSE 'Independent Dealer'
    END AS Supplier_type,

    -- ── Vehicle Age ───────────────────────────────────────────────────────────
    c.Model_Year,
    2015 - c.Model_Year AS vehicle_age,

    CASE
        WHEN 2015 - c.Model_Year <= 1  THEN 'New (0–1 yr)'
        WHEN 2015 - c.Model_Year <= 3  THEN 'Nearly New (2–3 yr)'
        WHEN 2015 - c.Model_Year <= 5  THEN 'Recent (4–5 yr)'
        WHEN 2015 - c.Model_Year <= 10 THEN 'Mid-Age (6–10 yr)'
        ELSE 'Older (10+ yr)'
    END AS age_bucket,

    -- ── Pricing & Revenue ─────────────────────────────────────────────────────
    c.Selling_Price,
    COUNT(*)                                                       AS total_sales,
    SUM(c.Selling_Price)                                           AS Revenue,
    ROUND(AVG(c.Selling_Price), 2)                                 AS avg_selling_price,
    percentile_approx(c.Selling_Price, 0.5)                        AS median_selling_price,

    -- ── Market Value ─────────────────────────────────────────────────────────
    c.Market_Value,
    coalesce(c.market_value,0)                                      AS Market_value2,

  CASE
    WHEN c.Selling_Price > COALESCE(c.Market_Value, 0) THEN 'Above Market_Value'
    WHEN c.Selling_Price < COALESCE(c.Market_Value, 0) THEN 'Below Market_Value'
    ELSE 'At Market_Value'
END AS market_position,

    COUNT(CASE WHEN c.Selling_Price > c.Market_Value THEN 1 END) AS Above_Market_value,
    COUNT(CASE WHEN c.Selling_Price < c.Market_Value THEN 1 END) AS below_Market_value,

    -- ── Mileage ───────────────────────────────────────────────────────────────
    c.Mileage,
    coalesce(c.mileage,0)                           AS Mileage2,

    CASE
        WHEN coalesce(c.mileage,0)  < 5000                             THEN 'New / Delivery'
        WHEN coalesce(c.mileage,0)   BETWEEN 5000     AND  20000       THEN 'Very Low'
        WHEN coalesce(c.mileage,0)   BETWEEN 20000    AND  40000       THEN 'Low'
        WHEN coalesce(c.mileage,0)   BETWEEN 40000    AND  65000       THEN 'Below Average'
        WHEN coalesce(c.mileage,0)   BETWEEN 65000    AND  90000       THEN 'Average / Medium'
        WHEN coalesce(c.mileage,0)   BETWEEN 90000    AND 120000       THEN 'Above Average'
        WHEN coalesce(c.mileage,0)   BETWEEN 120000   AND 160000       THEN 'High'
        WHEN coalesce(c.mileage,0)   BETWEEN 160000   AND 200000       THEN 'Very High'
        ELSE 'Extreme / High Mileage'
    END AS Mileage_condition,

    -- ── Condition ─────────────────────────────────────────────────────────────
    c.Condition_Score,
    coalesce(c.Condition_Score,0)                           AS condition_sore2,


    CASE
        WHEN  coalesce(c.Condition_Score,0)   >= 40 THEN 'Excellent'
        WHEN  coalesce(c.Condition_Score,0)   >= 30 THEN 'Good'
        WHEN  coalesce(c.Condition_Score,0)   >= 20 THEN 'Fair'
        WHEN  coalesce(c.Condition_Score,0)   >= 10 THEN 'Poor'
        ELSE 'Very Poor'
    END AS condition_tier,

    -- ── Profit Metrics ────────────────────────────────────────────────────────
    ROUND(c.Selling_Price - c.Market_Value, 2)                      AS profit_vs_marketv,
    ROUND(
        (c.Selling_Price - c.Market_Value)
        / NULLIF(c.Market_Value, 0) * 100,
    2)                                                              AS profit_margin_pct,

    CASE
        WHEN c.Market_Value IS NULL OR c.Market_Value = 0                                          THEN 'Unknown'
        WHEN ((c.Selling_Price - coalesce(c.market_value,0)   ) / coalesce(c.market_value,0)   ) * 100 >= 10                     THEN 'Premium (≥10%)'
        WHEN ((c.Selling_Price - coalesce(c.market_value,0)   ) / coalesce(c.market_value,0)   ) * 100 >= 5                      THEN 'Above Market (5% to 9.99%)'
        WHEN ((c.Selling_Price - coalesce(c.market_value,0)   ) / coalesce(c.market_value,0)   ) * 100 >= 0                      THEN 'Near Market (0% to 4.99%)'
        WHEN ((c.Selling_Price - coalesce(c.market_value,0)   ) / coalesce(c.market_value,0)   ) * 100 >= -5                     THEN 'Slight Discount (-5% to -0.01%)'
        ELSE 'Deep Discount (< -5%)'
    END AS margin_tier,

    ROUND(c.Selling_Price / NULLIF(c.Market_Value, 0), 4)           AS price_marketv_ratio,

    CASE
        WHEN ROUND(c.Selling_Price / NULLIF(c.Market_Value, 0), 4) > 1  THEN 'Selling Above Market Value (Strong Retention)'
        WHEN ROUND(c.Selling_Price / NULLIF(c.Market_Value, 0), 4) = 1  THEN 'Selling At Market Value'
        WHEN ROUND(c.Selling_Price / NULLIF(c.Market_Value, 0), 4) < 1  THEN 'Selling Below Market Value (Weaker Retention)'
        ELSE 'Unknown'
    END AS Retention,

    -- ── Date & Time Dimensions ────────────────────────────────────────────────
    
    DATE(c.Sale_Date)                                               AS sale_date,

    -- BigQuery: FORMAT_DATE('%A', date) → Databricks: date_format(date, 'EEEE')
    date_format(c.Sale_Date, 'EEEE')                                AS day_of_week,

    -- BigQuery: FORMAT_DATETIME('%H:%M:%S', ts) → Databricks: date_format(ts, 'HH:mm:ss')
    date_format(c.Sale_Date, 'HH:mm:ss')                            AS transaction_time,

    CASE
        WHEN date_format(c.Sale_Date, 'HH:mm:ss') BETWEEN '00:00:00' AND '11:59:59' THEN 'Morning'
        WHEN date_format(c.Sale_Date, 'HH:mm:ss') BETWEEN '12:00:00' AND '16:59:59' THEN 'Afternoon'
        WHEN date_format(c.Sale_Date, 'HH:mm:ss') BETWEEN '17:00:00' AND '21:59:59' THEN 'Evening'
        ELSE 'Night'
    END AS time_group,

    -- BigQuery: EXTRACT(DAYOFWEEK FROM date) → Databricks: dayofweek(date)
    -- Spark dayofweek(): 1 = Sunday … 7 = Saturday  (same convention as BigQuery)
    CASE
        WHEN dayofweek(DATE(c.Sale_Date)) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_category,

    -- BigQuery: FORMAT_DATE('%B', date) → Databricks: date_format(date, 'MMMM')
    date_format(c.Sale_Date, 'MMMM')                                AS month_name,
    COALESCE( date_format(c.Sale_Date, 'MMMM'),'Unknown')           AS month_name2,

    -- BigQuery: EXTRACT(DAY FROM date) → Databricks: dayofmonth(date)
    CASE
        WHEN dayofmonth(DATE(c.Sale_Date)) BETWEEN 1  AND 10 THEN 'Beginning of Month'
        WHEN dayofmonth(DATE(c.Sale_Date)) BETWEEN 11 AND 20 THEN 'Mid of Month'
        ELSE 'End of Month'
    END AS month_pattern

FROM carsales c

-- ── GROUP BY — explicit list (Databricks does not support GROUP BY ALL) ────────
GROUP BY ALL;

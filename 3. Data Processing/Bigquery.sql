# Table Overview
SELECT *
FROM `car-sales-analytics.Brightmotor.cars`
LIMIT 100;

#DATA INSPECTION



-- Sales Date Range
SELECT
    MIN(SAFE.PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate))) AS earliest_sale,
    MAX(SAFE.PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate))) AS latest_sale
FROM `car-sales-analytics.Brightmotor.cars`;



-- Total Rows / Distinct VINs / Duplicate VINs
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT vin) AS distinct_vins,
    COUNT(*) - COUNT(DISTINCT vin) AS duplicate_vin_rows
FROM `car-sales-analytics.Brightmotor.cars`;



-- Different Makes / Models / Sellers / States
SELECT 
      COUNT(DISTINCT make) AS total_makes,        
      COUNT(DISTINCT `model`) AS total_models,      
      COUNT(DISTINCT seller) AS total_sellers,      
      COUNT(DISTINCT state) AS total_states,       
 FROM `car-sales-analytics.Brightmotor.cars`;


-- Vehicle Year Range
SELECT
    MIN(SAFE_CAST(year AS INT64)) AS oldest_model_year,
    MAX(SAFE_CAST(year AS INT64)) AS newest_model_year
FROM `car-sales-analytics.Brightmotor.cars`;


-- Price/Revenue / MMR / Odometer Overview/Condition
SELECT
    MIN(SAFE_CAST(sellingprice AS INT64)) AS min_price,
    MAX(SAFE_CAST(sellingprice AS INT64)) AS max_price,
    ROUND(AVG(SAFE_CAST(sellingprice AS INT64)),0) AS avg_price,

    MIN(PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate))) AS start_SELLING,
    MAX(PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate))) AS stop_SELLING,

    SUM(sellingprice) AS Revenue,

    MIN(SAFE_CAST(mmr AS INT64)) AS min_mmr,
    MAX(SAFE_CAST(mmr AS INT64)) AS max_mmr,
    ROUND(AVG(SAFE_CAST(mmr AS INT64)),0) AS avg_mmr,

    MIN(SAFE_CAST(odometer AS INT64)) AS min_odometer,
    MAX(SAFE_CAST(odometer AS INT64)) AS max_odometer,
    ROUND(AVG(SAFE_CAST(odometer AS INT64)),0) AS avg_odometer,

    MIN(SAFE_CAST(condition AS INT64)) AS min_condition,
    MAX(SAFE_CAST(condition AS INT64))AS max_condition,
    ROUND(AVG(SAFE_CAST(condition AS INT64)),0)AS AVG_condition

FROM `car-sales-analytics.Brightmotor.cars`
WHERE sellingprice IS NOT NULL
AND condition IS NOT NULL
AND mmr IS NOT NULL
AND odometer IS NOT NULL;



-- DUPLICATE VIN CHECK

SELECT
    vin,
    COUNT(*) AS duplicate_count
FROM `car-sales-analytics.Brightmotor.cars`
GROUP BY vin
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- Profit

SELECT
        sellingprice,
        mmr,
  CASE
    WHEN mmr > 0 THEN ROUND(sellingprice / mmr,2)
    END AS price_to_mmr_ratio,
 
    ROUND(sellingprice - mmr, 2) AS profit_vs_mmr,
 
  CASE
    WHEN mmr > 0 THEN ROUND((sellingprice - mmr) / mmr * 100, 2)
    END AS profit_margin_pct,
 
  CASE
      WHEN sellingprice > 0 THEN ROUND((sellingprice - mmr) / sellingprice * 100, 2)
      END AS gross_margin_pct

  FROM `car-sales-analytics.Brightmotor.cars`;




-- NULL / BAD RECORD CHECK

SELECT *
FROM `car-sales-analytics.Brightmotor.cars`
WHERE vin IS NULL
   OR make IS NULL
   OR `model` IS NULL
   OR sellingprice IS NULL
   OR mmr IS NULL
   OR odometer IS NULL
   OR saledate IS NULL
   OR `year` IS NULL
   OR `trim` IS NULL
   OR body IS NULL
   OR transmission IS NULL
   OR state IS NULL
   OR condition IS NULL
   OR color IS NULL
   OR interior IS NULL
   OR seller IS NULL;


##  DATA CLEANING

SELECT
    
    TRIM(vin) AS Vin,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(make)), ''), '—'), 'Unknown')            AS Manufacture,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(`model`)), ''), '—'), 'Unknown')           AS Model,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(`trim`)), ''), '—'), 'Unknown')            AS Features,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(body)), ''), '—'), 'Unknown')            AS BodyType,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(transmission)), ''), '—'), 'Unknown')    AS Transmission,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(color)), ''), '—'), 'Unknown')           AS Ext_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(interior)), ''), '—'), 'Unknown')        AS Int_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(seller)), ''), '—'), 'Unknown')          AS Supplier,
    TRIM(LOWER(state)) AS state_code,

  CASE 
        -- 🇺🇸 United States
        WHEN TRIM(LOWER(state)) = 'al' THEN 'Alabama'
        WHEN TRIM(LOWER(state)) = 'az' THEN 'Arizona'
        WHEN TRIM(LOWER(state)) = 'ca' THEN 'California'
        WHEN TRIM(LOWER(state)) = 'co' THEN 'Colorado'
        WHEN TRIM(LOWER(state)) = 'fl' THEN 'Florida'
        WHEN TRIM(LOWER(state)) = 'ga' THEN 'Georgia'
        WHEN TRIM(LOWER(state)) = 'hi' THEN 'Hawaii'
        WHEN TRIM(LOWER(state)) = 'il' THEN 'Illinois'
        WHEN TRIM(LOWER(state)) = 'in' THEN 'Indiana'
        WHEN TRIM(LOWER(state)) = 'la' THEN 'Louisiana'
        WHEN TRIM(LOWER(state)) = 'ma' THEN 'Massachusetts'
        WHEN TRIM(LOWER(state)) = 'md' THEN 'Maryland'
        WHEN TRIM(LOWER(state)) = 'mi' THEN 'Michigan'
        WHEN TRIM(LOWER(state)) = 'mn' THEN 'Minnesota'
        WHEN TRIM(LOWER(state)) = 'mo' THEN 'Missouri'
        WHEN TRIM(LOWER(state)) = 'ms' THEN 'Mississippi'
        WHEN TRIM(LOWER(state)) = 'nc' THEN 'North Carolina'
        WHEN TRIM(LOWER(state)) = 'ne' THEN 'Nebraska'
        WHEN TRIM(LOWER(state)) = 'nj' THEN 'New Jersey'
        WHEN TRIM(LOWER(state)) = 'nm' THEN 'New Mexico'
        WHEN TRIM(LOWER(state)) = 'nv' THEN 'Nevada'
        WHEN TRIM(LOWER(state)) = 'ny' THEN 'New York'
        WHEN TRIM(LOWER(state)) = 'oh' THEN 'Ohio'
        WHEN TRIM(LOWER(state)) = 'ok' THEN 'Oklahoma'
        WHEN TRIM(LOWER(state)) = 'or' THEN 'Oregon'
        WHEN TRIM(LOWER(state)) = 'pa' THEN 'Pennsylvania'
        WHEN TRIM(LOWER(state)) = 'pr' THEN 'Puerto Rico'
        WHEN TRIM(LOWER(state)) = 'sc' THEN 'South Carolina'
        WHEN TRIM(LOWER(state)) = 'tn' THEN 'Tennessee'
        WHEN TRIM(LOWER(state)) = 'tx' THEN 'Texas'
        WHEN TRIM(LOWER(state)) = 'ut' THEN 'Utah'
        WHEN TRIM(LOWER(state)) = 'va' THEN 'Virginia'
        WHEN TRIM(LOWER(state)) = 'wa' THEN 'Washington'
        WHEN TRIM(LOWER(state)) = 'wi' THEN 'Wisconsin'
        
        -- 🇨🇦 Canada
        WHEN TRIM(LOWER(state)) = 'ab' THEN 'Alberta'
        WHEN TRIM(LOWER(state)) = 'ns' THEN 'Nova Scotia'
        WHEN TRIM(LOWER(state)) = 'on' THEN 'Ontario'
        WHEN TRIM(LOWER(state)) = 'qc' THEN 'Quebec'
        
        ELSE 'Unknown'
  END AS StateName,

  CASE 
      WHEN state IN ('ab','ns','on','qc') THEN 'Canada'
      ELSE 'USA'
  END AS Country,


    SAFE_CAST(year AS INT64) AS Model_Year,
    SAFE_CAST(condition AS INT64) AS Condition_Score,
    SAFE_CAST(odometer AS INT64) AS Mileage,
    SAFE_CAST(mmr AS INT64) AS Market_Value,
    SAFE_CAST(sellingprice AS INT64) AS Selling_Price,

PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate)) AS Sale_Date
FROM `car-sales-analytics.Brightmotor.cars`;











-- ==============================================================================================
-- MASTER QUERY : DATA CLEANING + FEATURE ENGINEERING + FULL BUSINESS METRICS
-- ==============================================================================================
WITH carsales AS (

SELECT DISTINCT
    `year`,
    TRIM(vin) AS Vin,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(make)), ''), '—'), 'Unknown')            AS Manufacture,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(`model`)), ''), '—'), 'Unknown')         AS `Model`,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(`trim`)), ''), '—'), 'Unknown')          AS Features,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(body)), ''), '—'), 'Unknown')            AS BodyType,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(transmission)), ''), '—'), 'Unknown')    AS Transmission,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(color)), ''), '—'), 'Unknown')           AS Ext_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(interior)), ''), '—'), 'Unknown')        AS Int_color,
    COALESCE(NULLIF(NULLIF(INITCAP(TRIM(seller)), ''), '—'), 'Unknown')          AS Supplier,
    TRIM(LOWER(state)) AS state_code,

  CASE 
        -- 🇺🇸 United States
        WHEN TRIM(LOWER(state)) = 'al' THEN 'Alabama'
        WHEN TRIM(LOWER(state)) = 'az' THEN 'Arizona'
        WHEN TRIM(LOWER(state)) = 'ca' THEN 'California'
        WHEN TRIM(LOWER(state)) = 'co' THEN 'Colorado'
        WHEN TRIM(LOWER(state)) = 'fl' THEN 'Florida'
        WHEN TRIM(LOWER(state)) = 'ga' THEN 'Georgia'
        WHEN TRIM(LOWER(state)) = 'hi' THEN 'Hawaii'
        WHEN TRIM(LOWER(state)) = 'il' THEN 'Illinois'
        WHEN TRIM(LOWER(state)) = 'in' THEN 'Indiana'
        WHEN TRIM(LOWER(state)) = 'la' THEN 'Louisiana'
        WHEN TRIM(LOWER(state)) = 'ma' THEN 'Massachusetts'
        WHEN TRIM(LOWER(state)) = 'md' THEN 'Maryland'
        WHEN TRIM(LOWER(state)) = 'mi' THEN 'Michigan'
        WHEN TRIM(LOWER(state)) = 'mn' THEN 'Minnesota'
        WHEN TRIM(LOWER(state)) = 'mo' THEN 'Missouri'
        WHEN TRIM(LOWER(state)) = 'ms' THEN 'Mississippi'
        WHEN TRIM(LOWER(state)) = 'nc' THEN 'North Carolina'
        WHEN TRIM(LOWER(state)) = 'ne' THEN 'Nebraska'
        WHEN TRIM(LOWER(state)) = 'nj' THEN 'New Jersey'
        WHEN TRIM(LOWER(state)) = 'nm' THEN 'New Mexico'
        WHEN TRIM(LOWER(state)) = 'nv' THEN 'Nevada'
        WHEN TRIM(LOWER(state)) = 'ny' THEN 'New York'
        WHEN TRIM(LOWER(state)) = 'oh' THEN 'Ohio'
        WHEN TRIM(LOWER(state)) = 'ok' THEN 'Oklahoma'
        WHEN TRIM(LOWER(state)) = 'or' THEN 'Oregon'
        WHEN TRIM(LOWER(state)) = 'pa' THEN 'Pennsylvania'
        WHEN TRIM(LOWER(state)) = 'pr' THEN 'Puerto Rico'
        WHEN TRIM(LOWER(state)) = 'sc' THEN 'South Carolina'
        WHEN TRIM(LOWER(state)) = 'tn' THEN 'Tennessee'
        WHEN TRIM(LOWER(state)) = 'tx' THEN 'Texas'
        WHEN TRIM(LOWER(state)) = 'ut' THEN 'Utah'
        WHEN TRIM(LOWER(state)) = 'va' THEN 'Virginia'
        WHEN TRIM(LOWER(state)) = 'wa' THEN 'Washington'
        WHEN TRIM(LOWER(state)) = 'wi' THEN 'Wisconsin'
        
        -- 🇨🇦 Canada
        WHEN TRIM(LOWER(state)) = 'ab' THEN 'Alberta'
        WHEN TRIM(LOWER(state)) = 'ns' THEN 'Nova Scotia'
        WHEN TRIM(LOWER(state)) = 'on' THEN 'Ontario'
        WHEN TRIM(LOWER(state)) = 'qc' THEN 'Quebec'
        
        ELSE 'Unknown'
  END AS StateName,


    SAFE_CAST(year AS INT64) AS Model_Year,
    SAFE_CAST(condition AS INT64) AS Condition_Score,
    SAFE_CAST(odometer AS INT64) AS Mileage,
    SAFE_CAST(mmr AS INT64) AS Market_Value,
    SAFE_CAST(sellingprice AS INT64) AS Selling_Price,
    PARSE_DATETIME('%a %b %d %Y %H:%M:%S', TRIM(saledate)) AS Sale_Date
FROM `car-sales-analytics.Brightmotor.cars`
)


SELECT
    c.vin,
    c.Manufacture,
    c.model,
    c.Features,
    c.BodyType,

     CASE
            WHEN LOWER(BodyType) LIKE '%suv%' THEN 'SUV'
            WHEN LOWER(BodyType) LIKE '%sedan%' THEN 'Sedan'
            WHEN LOWER(BodyType) LIKE '%truck%' THEN 'Truck'
            WHEN LOWER(BodyType) LIKE '%coupe%' THEN 'Coupe'
            WHEN LOWER(BodyType) LIKE '%hatchback%' THEN 'Hatchback'
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

  CASE
    /* Rental Fleet */
    WHEN LOWER(c.Supplier) IN ('%hertz%','%avis%','%budget%','%enterprise%','%u-haul%','%dtg operations%') THEN 'Rental Fleet'

    /* Finance / Lease */
    WHEN LOWER(c.Supplier) IN ('%wells fargo%','%jpmorgan chase%','%chase%','%santander%','%tdaf%','%world omni%','%ally financial%','%gm financial%','%toyota financial%','%nissan financial%') THEN 'Finance / Lease'

    /* Manufacturer / OEM */
    WHEN LOWER(c.Supplier) IN ('%ford%','%toyota%','%honda%','%kia%','%nissan%','%hyundai%','%gm%','%volkswagen%','%mercedes%') THEN 'Manufacturer / OEM'

    /* Corporate Fleet */
    WHEN LOWER(c.Supplier) IN ('%ari fleet%','%wheels%','%leaseplan%','%element fleet%','%donlen%') THEN 'Corporate Fleet'

    /* Dealer / Wholesale */
    WHEN LOWER(c.Supplier) IN ('%auto sales%','%wholesale%','%motors%','%cars llc%','%auto inc%') THEN 'Dealer / Wholesale'

    WHEN c.Supplier='Unknown' THEN 'Unknown'

    ELSE 'Independent Dealer'
  END AS Supplier_type,

    c.model_year,
    2015 - c.model_year AS vehicle_age,
    
     CASE
          WHEN 2015 - c.model_year <= 1    THEN 'New (0–1 yr)'
          WHEN 2015 - c.model_year <= 3    THEN 'Nearly New (2–3 yr)'
          WHEN 2015 - c.model_year <= 5    THEN 'Recent (4–5 yr)'
          WHEN 2015 - c.model_year <= 10   THEN 'Mid-Age (6–10 yr)'
          ELSE 'Older (10+ yr)'
      END AS age_bucket,

    
    c.selling_price,
    COUNT (*) AS total_sales,
    SUM(selling_price) AS Revenue,
    ROUND(AVG(selling_price),2) AS avg_selling_price,
    APPROX_QUANTILES(selling_price,2)[OFFSET(1)] AS median_selling_price,


    c.Market_Value,

     CASE
          WHEN selling_price > c.Market_Value THEN 'Above Market_Value'
          WHEN selling_price < c.Market_Value THEN 'Below Market_Value'
          ELSE 'At Market_Value'
    END AS market_position,
    COUNT(selling_price > c.Market_Value) Above_Market_value,
    COUNT(selling_price < c.Market_Value) below_Market_value,

    c.Mileage,

     CASE
          WHEN c.Mileage < 5000 THEN 'New / Delivery'
          WHEN c.Mileage BETWEEN 5000 AND  20000 THEN 'Very Low'
          WHEN c.Mileage BETWEEN 20000 AND 40000 THEN 'Low'
          WHEN c.Mileage BETWEEN 40000 AND 65000 THEN 'Below Average'
          WHEN c.Mileage BETWEEN 65000 AND 90000 THEN 'Average / Medium'
          WHEN c.Mileage BETWEEN 90000 AND 120000 THEN 'Above Average'
          WHEN c.Mileage BETWEEN 120000 AND 160000 THEN 'High'
          WHEN c.Mileage BETWEEN 160000 AND 200000 THEN 'Very High'
          ELSE 'Extreme / High Mileage'
    END AS Mileage_condition,


    c.condition_score,
    CASE 
         WHEN c.condition_score >= 40 THEN 'Excellent'
         WHEN c.condition_score >= 30 THEN 'Good'
         WHEN c.condition_score >= 20 THEN 'Fair'
         WHEN c.condition_score >= 10 THEN 'Poor'
         ELSE 'Very Poor' 
    END AS condition_tier,

    ROUND(c.selling_price - c.Market_Value,2) AS profit_vs_marketv,
    ROUND((c.selling_price - c.Market_Value)/c.Market_Value*100,2) AS profit_margin_pct,
    
  CASE
    WHEN c.Market_Value IS NULL OR c.Market_Value = 0 THEN 'Unknown'
    WHEN ((c.selling_price - c.Market_Value) / c.Market_Value) * 100 >= 10 THEN 'Premium (≥10%)'
    WHEN ((c.selling_price - c.Market_Value) / c.Market_Value) * 100 >= 5 THEN 'Above Market (5% to 9.99%)'
    WHEN ((c.selling_price - c.Market_Value) / c.Market_Value) * 100 >= 0 THEN 'Near Market (0% to 4.99%)'
    WHEN ((c.selling_price - c.Market_Value) / c.Market_Value) * 100 >= -5 THEN 'Slight Discount (-5% to -0.01%)'
    ELSE 'Deep Discount (< -5%)'
  END AS margin_tier,

    ROUND(c.selling_price/c.Market_Value,4) AS price_marketv_ratio,

 CASE
    WHEN ROUND(SAFE_DIVIDE(c.selling_price, c.Market_Value), 4) > 1 THEN 'Selling Above Market Value (Strong Retention)'   WHEN ROUND(SAFE_DIVIDE(c.selling_price, c.Market_Value), 4) = 1 THEN 'Selling At Market Value'
    WHEN ROUND(SAFE_DIVIDE(c.selling_price, c.Market_Value), 4) < 1 THEN 'Selling Below Market Value (Weaker Retention)'
    ELSE 'Unknown'
END AS Retention,


    DATE(c.sale_date) AS sale_date,
    
    
    FORMAT_DATE('%A', DATE(c.sale_date)) AS day_of_week,
    FORMAT_DATETIME('%H:%M:%S', c.sale_date) AS transaction_time, 
    
    CASE
        WHEN FORMAT_DATETIME('%H:%M:%S', c.sale_date) BETWEEN '00:00:00' AND '11:59:59' THEN 'Morning'
        WHEN FORMAT_DATETIME('%H:%M:%S', c.sale_date) BETWEEN '12:00:00' AND '16:59:59' THEN 'Afternoon'
        WHEN FORMAT_DATETIME('%H:%M:%S', c.sale_date) BETWEEN '17:00:00' AND '21:59:59' THEN 'Evening'
        ELSE 'Night'
    END AS time_group,
    
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM DATE(c.sale_date)) IN (6,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_category,
    
    FORMAT_DATE('%B', DATE(c.sale_date)) AS month_name,

    CASE 
         WHEN EXTRACT(DAY FROM DATE(c.sale_date)) BETWEEN 1 AND 10 THEN 'Beginning of Month'
         WHEN EXTRACT(DAY FROM DATE(c.sale_date)) BETWEEN 11 AND 20 THEN 'Mid of Month'
         ELSE 'End of Month' 
    END AS month_pattern
    
   
FROM carsales c
GROUP BY ALL;




























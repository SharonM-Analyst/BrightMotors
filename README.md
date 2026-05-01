# 🚗 Bright Motors Automotive Sales Intelligence
---

## Overview  

This case study analyses over 558,811 vehicle sales records across the USA and Canada to uncover dealership profitability gaps, benchmark pricing against market value, optimise suppliers performance, and identify the most profitable automotive segments in the U.S. resale market.

The project was designed to simulate a real-world business intelligence engagement producing executive-grade dashboards, investor-style insights, and strategic recommendations that support commercial decision-making for automotive dealers, suppliers, and investors.

**Business Problem:**  
Despite generating **$7.61 billion in revenue** with 558,811 transactions and an average selling price of $13,611, Bright Motors recorded an overall **net loss of $88.3 million** indicating significant margin pressure driven by underpricing, inefficient seller practices, and poor inventory management.


## Methodology: How the Case Study Was Done


### Data Ingestion & Setup
- Sourced raw vehicle auction sales data in CSV format
- Set up the project environment with Google BigQuery as the cloud data warehouse
- Loaded and uploaded raw CSV files into BigQuery, defining schema and data types
- Normalised delimiters, encoding formats, and validated row counts using SQL

### Data Inspection & Profiling
- Performed row count and schema checks using `COUNT`, `COUNT DISTINCT`, and `INFORMATION_SCHEMA`
- Audited for null and blank values across all critical fields
- Computed numeric range statistics (`MIN`, `MAX`, `AVG`, `STDDEV`) to detect outliers
- Identified and flagged duplicate records using `GROUP BY` and `HAVING COUNT > 1`
- Applied a quality acceptance gate only proceeding when data quality passed defined thresholds

### Data Cleaning (SQL-based, CTE-driven)
- Built a `raw_parsed_CTE` to standardise date formats and cast data types
- Applied **string cleaning** using `CAST`, `TRIM`, and `INITCAP`
- Performed **numeric validation** with `CASE WHEN` guards to handle invalid ranges
- Created a `deduped_CTE` to remove exact duplicate rows
- Produced a final `cleaned CTE` with quality flags (TRUE/FALSE) for downstream use
- Output: **~550,000+ clean, validated records** ready for analysis

### Feature Engineering (CTEs built in BigQuery)
- **Price & Margin Features:** `profit_margin`, `market_value_gap`, `price_ratio`, `margin_tier`
- **Vehicle Features:** `vehicle_age`, `age_bucket`, `mileage_bucket`, `body_type_clean`
- **Time Buckets:** `sale_month`, `sale_quarter`, `sale_day_of_week`, `week_start`
- **Seller & MMR Position Flags:** `above_mmr`, `below_mmr`, `mmr_position`, `seller_type_clean`

### Multi-Dimensional Analysis & KPI Computation
- **Pricing & Market Intelligence:** Avg margin (-0.66%), avg ratio (0.99), margin tier distributions
- **Time & Seller Analysis:** Revenue and profit trends by month, quarter, and seller type
- **Depreciation & Vehicle Lifecycle:** Revenue and pricing by vehicle age bucket (New → Older)
- Built a **KPI Summary Dashboard View** in BigQuery feeding directly into Power BI

### Dashboard Development (Data Studio: https://datastudio.google.com/s/h8FqqfyM4Os  Loveable:https://sharonmbright.lovable.app )

Built a **5-page executive Power BI dashboard** with dynamic slicers (Date, Location, Manufacturer, Body Design, Transmission, Color):

1. **Executive Summary** — Total sales, revenue, profit/loss, avg price, profit & revenue trend, manufacturer contribution, geographic distribution
2. **Vehicle Insights** — Sales by vehicle shape, revenue by model & transmission, mileage vs price, vehicle lifecycle performance
3. **Pricing Intelligence** — Avg margin, avg ratio, margin tier breakdown, brand-level pricing performance, above/below market value split
4. **Supplier Performance** — Supplier leaderboard (14,261 suppliers), profit margin vs revenue, avg selling price by supplier
5. **Geographic Trends** — Revenue & loss by state, pricing breakdown by state, margin tier distribution by state

### Presentation & Recommendations
- Produced an **investor-style PowerPoint presentation** (Bright Motors Strategic Turnaround)
- Documented findings as actionable commercial insights
- Built a complete **SVG pipeline flowchart** and **Gantt chart** to document the end-to-end methodology

---

## 📊 Key Insights Found

### 1. Revenue Without Profit — A Systemic Pricing Problem
Despite $7.61 billion in revenue and 558,811 transactions, the business recorded an **$88.3 million net loss**, driven by an average profit margin of **-0.66%** and a price-to-market ratio of just **0.99** — meaning vehicles are being sold below market value on average.

### 2. Pricing Gap: 50.4% of Vehicles Sold Below Market Value
- **50.4%** of all vehicles were sold **below market value**
- Only **47.2%** were sold above market value
- Just **2.4%** transacted at exact market value
- This pricing gap is the single largest driver of losses across the portfolio

### 3. Ford Leads Revenue but Struggles with Profitability
- **Ford Motor Credit Company LLC** ranks #1 by revenue and total sales volume
- However, profit margins across top brands remain negative or near-zero
- **Nissan-Infiniti LT** and **The Hertz Corporation** follow closely but show similar margin compression

### 4. Vehicle Lifecycle Destroys Value for Older Stock
- **"Nearly New" (2–3 yr)** vehicles generated the highest revenue ($3.5bn+) and commanded the best average prices
- Revenue drops sharply for **"Mid-Age" (6–10 yr)** and **"Older" (10+ yr)** vehicles, with average price declining steeply
- Dealers holding aging inventory face compounding depreciation losses

### 5. Sedans and SUVs Dominate Sales Volume
- **Sedans** represent the largest share of transactions by vehicle shape
- **SUVs** follow as the second largest contributor
- Niche body types (Coupe, Wagon, Convertible) represent a small but potentially higher-margin opportunity

### 6. Geographic Concentration in Florida, California & Texas
- **Florida** leads all states in total sales volume and revenue, followed by California, Pennsylvania, and Texas
- These states also show the highest **above-market vs below-market pricing variance**
- **Nova Scotia (Canada)** shows significantly smaller volumes — indicating the market is overwhelmingly USA-driven

### 7. Deep Discount Sellers Are Destroying Portfolio Margins
- Suppliers in the **"Deep Discount" margin tier (< -5%)** are pulling down overall profitability
- Several high-revenue suppliers (e.g., Ford Motor, Financial Services Remarketing) show **negative profit margin percentages** despite large transaction volumes
- **Phelps Automotive** and **Financial Services** command the highest average selling prices, suggesting a premium-tier opportunity

---

## ✅ Recommendations

### 1. Implement a Minimum Pricing Floor Policy
Set a **minimum price ratio of 1.00** (at or above MMR/market value) as a mandatory seller requirement across all auction listings. Eliminating below-market pricing on the 50.4% underpriced inventory would directly recover margin losses.

### 2. Prioritise Nearly New Inventory (2–3 Year Vehicles)
Shift acquisition strategy toward **2–3 year old vehicles**, which deliver the highest revenue and best price retention. Reduce exposure to vehicles older than 10 years, which generate minimal revenue per unit and compress portfolio margins.

### 3. Segment and Reward High-Performing Suppliers
Introduce a **Supplier Tier Programme** based on profit margin performance:
- **Platinum Tier:** Above-market pricing suppliers: provide preferred auction slots and incentives
- **Standard Tier:** Near-market suppliers: maintain current terms
- **Review Tier:** Deep-discount suppliers: require pricing compliance training or contract review

### 4. Geographic Pricing Optimisation
Apply **state-level dynamic pricing benchmarks** particularly in Florida and California, where the largest above-market vs below-market gaps exist. Local market intelligence should inform reserve price setting by state.

### 5. Expand Premium Segment Inventory
Vehicles from premium suppliers (avg price >$100k) show stronger margin potential. Increasing the share of **luxury and premium brand listings** (BMW, Mercedes-Benz, Porsche Finance) would diversify and elevate the overall portfolio average selling price.

### 6. Automate Pricing Intelligence with Real-Time MMR Monitoring
Build an automated **Price Intelligence Dashboard** (Power BI + BigQuery pipeline) that flags any listing priced more than 5% below market value before it goes live — preventing deep discounts before they occur rather than analysing them after.

---

## 🗓️ Implementation Plan

| Phase | Action | Timeline | Owner |
|---|---|---|---|
| **Phase 1** | Enforce minimum pricing floor (ratio ≥ 1.00) across all listings | Week 1–2 | Pricing & Operations |
| **Phase 2** | Launch Supplier Tier Programme with scoring criteria | Week 2–4 | Supplier Relations |
| **Phase 3** | Shift acquisition budget 30% toward 2–3 year old inventory | Week 3–6 | Inventory & Procurement |
| **Phase 4** | Deploy state-level dynamic pricing benchmarks in Power BI | Week 4–6 | BI & Analytics Team |
| **Phase 5** | Automate MMR monitoring alert system (BigQuery + Power Automate) | Week 6–10 | Data Engineering |
| **Phase 6** | Expand premium/luxury segment listings by 15% | Week 8–12 | Business Development |
| **Phase 7** | Quarterly KPI review — margin, revenue, supplier performance | Ongoing | Leadership & Analytics |

**Target Outcome:** Recover $40–60M of the $88.3M loss within 12 months through pricing discipline, supplier optimisation, and inventory strategy realignment.

---

## 📁 Project Deliverables

| Deliverable | Description |
|---|---|
| `Car_Sales_Dashboard.pbix` | 5-page executive Power BI dashboard |
| `Car_Sales_Dashboard.pdf` | PDF export of the Power BI dashboard |
| `Bright_Motors_Strategic_Turnaround.pptx` | Investor-style PowerPoint presentation |
| `car_sales_pipeline_flowchart.svg` | End-to-end SQL pipeline flowchart |
| `Gantt_Chart_Car_Sales.png` | Project planning Gantt chart |

## Tools Used In Overall Case Study
* SQL
* Databricks
* NotebookLM
* Power BI & Data Studio
* Microsoft Excel
* Mirrow & Canva_Planning & Presentation




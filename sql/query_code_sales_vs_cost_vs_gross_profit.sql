USE mckinsey;

-- =============================================================================
-- 1. DATA INSPECTION
-- =============================================================================
-- 1.1 Row count: how many financial records are available for analysis
SELECT COUNT(*) AS total_rows
FROM `Financial Figures`;

-- 1.2 Missing values check: ensure key metrics are complete (Sales, Cost, Gross Profit)
SELECT
  SUM(CASE WHEN `Sales` IS NULL THEN 1 ELSE 0 END)        AS null_sales,
  SUM(CASE WHEN `Cost` IS NULL THEN 1 ELSE 0 END)         AS null_cost,
  SUM(CASE WHEN `Gross Profit` IS NULL THEN 1 ELSE 0 END) AS null_profit
FROM `Financial Figures`;

-- 1.3 Loss check: count rows where Gross Profit is negative
SELECT COUNT(*) AS loss_rows
FROM `Financial Figures`
WHERE `Gross Profit` < 0;

-- 1.4 Break-even check: count rows where Gross Profit is exactly zero
SELECT COUNT(*) AS zero_profit_rows
FROM `Financial Figures`
WHERE `Gross Profit` = 0;

-- 1.5 Sanity check: count rows where Cost exceeds Sales (would imply negative margin)
SELECT COUNT(*) AS cost_greater_than_sales
FROM `Financial Figures`
WHERE `Cost` > `Sales`;


-- =============================================================================
-- 2. DATA CLEANING (practical, non-destructive)
-- =============================================================================
-- Instead of updating the base table, we create a "clean" view:
-- - standardizes data types
-- - computes gross margin safely (avoids division by zero)
-- - keeps the original table unchanged

-- 2.1 Clean view: cast numeric fields and add a gross margin percentage column
CREATE VIEW v_financial_figures_clean AS
SELECT
  `Financial Figures ID`,                          -- surrogate key
  `Order ID`,                                      -- FK to Order (proxy)
  `Factory ID`,                                    -- FK to Factory
  `Product ID`,                                    -- FK to Product
  CAST(`Units` AS SIGNED)             AS `Units`,   -- enforce integer type
  CAST(`Sales` AS DECIMAL(12,2))      AS `Sales`,   -- standardize decimals
  CAST(`Cost` AS DECIMAL(12,2))       AS `Cost`,
  CAST(`Gross Profit` AS DECIMAL(12,2)) AS `Gross Profit`,
  CASE
    WHEN `Sales` IS NULL OR `Sales` = 0 THEN NULL  -- avoid divide-by-zero / undefined margin
    ELSE ROUND(100 * (`Gross Profit` / `Sales`), 2)
  END AS gross_margin_pct
FROM `Financial Figures`;

-- =============================================================================
-- 3. ANALYSIS – OVERALL PROFITABILITY (no Loss)
-- =============================================================================

-- 3.1 Overall KPI totals: total Sales, total Cost, total Gross Profit, and gross margin %
SELECT
  ROUND(SUM(`Sales`), 2)        AS total_sales,
  ROUND(SUM(`Cost`), 2)         AS total_cost,
  ROUND(SUM(`Gross Profit`), 2) AS total_gross_profit,
  ROUND(100 * SUM(`Gross Profit`) / NULLIF(SUM(`Sales`),0), 2) AS gross_margin_pct
FROM v_financial_figures_clean;

-- 3.2 Margin distribution: how many records fall into each margin band (and their total profit)
SELECT
  CASE
    WHEN gross_margin_pct < 10 THEN '<10%'
    WHEN gross_margin_pct < 20 THEN '10–19.99%'
    WHEN gross_margin_pct < 30 THEN '20–29.99%'
    WHEN gross_margin_pct < 40 THEN '30–39.99%'
    ELSE '>=40%'
  END AS margin_band,
  COUNT(*) AS num_records,                         -- record count per band
  ROUND(SUM(`Gross Profit`), 2) AS total_profit     -- profit contribution per band
FROM v_financial_figures_clean
GROUP BY margin_band
ORDER BY
  CASE margin_band                                 -- custom sort order for bands
    WHEN '<10%' THEN 1
    WHEN '10–19.99%' THEN 2
    WHEN '20–29.99%' THEN 3
    WHEN '30–39.99%' THEN 4
    ELSE 5
  END;

-- Quick check table min marging, max margin, avg. margin
SELECT
  ROUND(MIN(gross_margin_pct), 2) AS min_margin_pct,
  ROUND(MAX(gross_margin_pct), 2) AS max_margin_pct,
  ROUND(AVG(gross_margin_pct), 2) AS avg_margin_pct
FROM v_financial_figures_clean;

-- Quick check to margin less than 10 (lt_10), margin between 10 and 40 (btw_10_40), margin greather than or equel to 40 (ge_40)
SELECT
  SUM(CASE WHEN gross_margin_pct < 10 THEN 1 ELSE 0 END) AS lt_10,
  SUM(CASE WHEN gross_margin_pct >= 10 AND gross_margin_pct < 40 THEN 1 ELSE 0 END) AS btw_10_40,
  SUM(CASE WHEN gross_margin_pct >= 40 THEN 1 ELSE 0 END) AS ge_40
FROM v_financial_figures_clean;

-- Quick check 
SELECT COUNT(*) AS margin_over_100
FROM v_financial_figures_clean
WHERE gross_margin_pct > 100;

-- Quick deep check into gross margin
SELECT
  CASE
    WHEN gross_margin_pct >= 40 AND gross_margin_pct < 50 THEN '40–49.99%'
    WHEN gross_margin_pct >= 50 AND gross_margin_pct < 60 THEN '50–59.99%'
    WHEN gross_margin_pct >= 60 AND gross_margin_pct < 70 THEN '60–69.99%'
    WHEN gross_margin_pct >= 70 AND gross_margin_pct < 80 THEN '70–79.99%'
    WHEN gross_margin_pct >= 80 AND gross_margin_pct < 90 THEN '80–89.99%'
    WHEN gross_margin_pct >= 90 AND gross_margin_pct <= 100 THEN '90–100%'
  END AS margin_sub_band,
  COUNT(*) AS num_records,
  ROUND(SUM(`Gross Profit`), 2) AS total_profit
FROM v_financial_figures_clean
WHERE gross_margin_pct >= 40
GROUP BY margin_sub_band
ORDER BY
  CASE margin_sub_band
    WHEN '40–49.99%' THEN 1
    WHEN '50–59.99%' THEN 2
    WHEN '60–69.99%' THEN 3
    WHEN '70–79.99%' THEN 4
    WHEN '80–89.99%' THEN 5
    WHEN '90–100%' THEN 6
  END;

-- =============================================================================
-- 4. DATASET FOR VISUALIZATION (slide-ready)
-- =============================================================================

-- 4.1 Chart dataset: Overall Sales vs Cost vs Gross Profit (3 bars)
-- For a simple bar chart: Sales, Cost, Gross Profit
SELECT 'Sales' AS metric, ROUND(SUM(`Sales`), 2) AS amount
FROM v_financial_figures_clean
UNION ALL
SELECT 'Cost', ROUND(SUM(`Cost`), 2)
FROM v_financial_figures_clean
UNION ALL
SELECT 'Gross Profit', ROUND(SUM(`Gross Profit`), 2)
FROM v_financial_figures_clean;

-- -----------------------------------------------------------------------------------
-- 4.2 KPI dataset: Overall Gross Margin %
-- To show a headline KPI for the slide 
SELECT
  ROUND(100 * SUM(`Gross Profit`) / NULLIF(SUM(`Sales`),0), 2) AS gross_margin_pct
FROM v_financial_figures_clean;

-- -------------------------------------------------------------------------
-- 4.3 Chart dataset: margin buckets or bands (<10%, 10–39.99%, >=40%)
-- To show low vs medium vs high margin structure at a high level
SELECT
  CASE
    WHEN gross_margin_pct < 10 THEN '<10%'
    WHEN gross_margin_pct >= 10 AND gross_margin_pct < 40 THEN '10–39.99%'
    ELSE '>=40%'
  END AS margin_group,
  COUNT(*) AS num_records,
  ROUND(SUM(`Gross Profit`), 2) AS total_profit
FROM v_financial_figures_clean
GROUP BY margin_group
ORDER BY
  CASE margin_group
    WHEN '<10%' THEN 1
    WHEN '10–39.99%' THEN 2
    ELSE 3
  END;

-- -----------------------------------------------------
-- 4.4 Reference table of margin bands
-- To show overall margin volume
-- ---------------------------------------------------------
  /* 1) Define a fixed reference table of margin bands
      This ensures that ALL margin ranges appear in the output,
      even if there are no transactions in some bands as in our case (band 10-39 %)
   --------------------------------------------------------- */
WITH margin_bands AS (
  SELECT '0–9.99%'   AS margin_band, 0  AS min_pct, 10 AS max_pct, 0  AS sort_band UNION ALL
  SELECT '10–19.99%', 10, 20, 10 UNION ALL
  SELECT '20–29.99%', 20, 30, 20 UNION ALL
  SELECT '30–39.99%', 30, 40, 30 UNION ALL
  SELECT '40–49.99%', 40, 50, 40 UNION ALL
  SELECT '50–59.99%', 50, 60, 50 UNION ALL
  SELECT '60–69.99%', 60, 70, 60 UNION ALL
  SELECT '70–79.99%', 70, 80, 70 UNION ALL
  SELECT '80–89.99%', 80, 90, 80 UNION ALL
  SELECT '90–100%',   90, 101, 90
),

/* ---------------------------------------------------------
   2) Base dataset
      Pull only the fields needed for margin analysis
      No filtering here — we want full coverage
   --------------------------------------------------------- */
base AS (
  SELECT
    gross_margin_pct,
    `Gross Profit`
  FROM v_financial_figures_clean
),

/* ---------------------------------------------------------
   3) Aggregate financial data by margin band
      LEFT JOIN is critical:
      - Keeps bands with zero transactions
      - Produces num_records = 0 where no data exists
   --------------------------------------------------------- */
aggregated AS (
  SELECT
    mb.margin_band,
    mb.sort_band,
    COUNT(f.gross_margin_pct) AS num_records,        -- Number of transactions per band
    ROUND(SUM(f.`Gross Profit`), 2) AS total_profit, -- Total gross profit per band
    ROUND(AVG(f.gross_margin_pct), 2) AS avg_margin_pct -- Average margin within the band
  FROM margin_bands mb
  LEFT JOIN base f
    ON f.gross_margin_pct >= mb.min_pct
   AND f.gross_margin_pct <  mb.max_pct
  GROUP BY mb.margin_band, mb.sort_band
)

/* ---------------------------------------------------------
   4) Final output
      - Shows absolute volume (num_records)
      - Shows relative volume (% of total transactions)
      - Ensures profit is 0 for empty bands
      - Orders bands logically from low to high margin
   --------------------------------------------------------- */
SELECT
  margin_band,
  num_records,
  ROUND(
    100.0 * num_records / SUM(num_records) OVER (),
    2
  ) AS pct_transaction_volume,   -- % of transactions in each margin band
  COALESCE(total_profit, 0) AS total_profit,
  avg_margin_pct
FROM aggregated
ORDER BY sort_band ASC;

-- ---------------------------------------------------------------------
-- 4.5 Chart dataset: Margin sub-bands within >=40% (10pp buckets)
-- Use this for a distribution chart to show where profit concentrates (60–79% bands)
-- Sub-band distribution inside the >=40% margin group answers:
-- - Is profit concentrated in "good" margins (40–60) or "excellent" margins (70%+)?
-- - Are there outliers inflating the results?

SELECT
  CASE
    WHEN gross_margin_pct >= 40 AND gross_margin_pct < 50 THEN '40–49.99%'
    WHEN gross_margin_pct >= 50 AND gross_margin_pct < 60 THEN '50–59.99%'
    WHEN gross_margin_pct >= 60 AND gross_margin_pct < 70 THEN '60–69.99%'
    WHEN gross_margin_pct >= 70 AND gross_margin_pct < 80 THEN '70–79.99%'
    WHEN gross_margin_pct >= 80 AND gross_margin_pct < 90 THEN '80–89.99%'
    WHEN gross_margin_pct >= 90 AND gross_margin_pct <= 100 THEN '90–100%'
  END AS margin_sub_band,

  COUNT(*) AS num_records,

  -- Transaction volume as % of total records in the filtered dataset (>=40% and <=100%)
  ROUND(
    100.0 * COUNT(*) / (SELECT COUNT(*) 
                        FROM v_financial_figures_clean 
                        WHERE gross_margin_pct >= 40 AND gross_margin_pct <= 100),
    2
  ) AS pct_transaction_volume,

  ROUND(SUM(`Gross Profit`), 2) AS total_profit,
  ROUND(AVG(gross_margin_pct), 2) AS avg_margin_pct,

  -- Helper numeric value for sorting (lower bound of each band)
  MIN(gross_margin_pct) AS sort_margin

FROM v_financial_figures_clean
WHERE gross_margin_pct >= 40
  AND gross_margin_pct <= 100

GROUP BY margin_sub_band

ORDER BY sort_margin ASC;

-- -------------------------------------------------------------------
-- 4.6 Chart dataset: Margin sub-bands within >=40% (10pp buckets)
-- Use this to rank the top 10 products by total Gross Profit within the high-margin segment (≥40%)
-- Top 10 products among ALL transactions with gross margin >= 40%
-- Ranking by total gross profit (most profit contributors first)

WITH base AS (
  -- Base dataset restricted to high-margin transactions (>= 40%)
  SELECT
    ffc.`Product ID`,
    ffc.`Sales`,
    ffc.`Cost`,
    ffc.`Gross Profit`
  FROM v_financial_figures_clean ffc
  WHERE ffc.gross_margin_pct >= 40
    AND ffc.gross_margin_pct <= 100
),

total_volume AS (
  -- Total number of transactions in the >=40% margin universe
  SELECT COUNT(*) AS total_transactions
  FROM base
),

aggregated AS (
  -- Aggregate metrics at product level
  SELECT
    p.`Product Name`,
    b.`Product ID`,

    COUNT(*) AS num_records,

    ROUND(
      100.0 * COUNT(*) / tv.total_transactions,
      2
    ) AS pct_transaction_volume,

    ROUND(SUM(b.`Cost`), 2) AS total_cost,
    ROUND(SUM(b.`Sales`), 2) AS sales,
    ROUND(SUM(b.`Gross Profit`), 2) AS gross_profit,

    ROUND(
      100 * SUM(b.`Gross Profit`) / NULLIF(SUM(b.`Sales`), 0),
      2
    ) AS gross_margin_pct

  FROM base b
  JOIN `Product` p
    ON p.`Product ID` = b.`Product ID`
  CROSS JOIN total_volume tv

  GROUP BY
    p.`Product Name`,
    b.`Product ID`,
    tv.total_transactions
),

ranked AS (
  -- Add ranking by gross profit
  SELECT
    *,
    RANK() OVER (ORDER BY gross_profit DESC) AS profit_rank
  FROM aggregated
)

SELECT *
FROM ranked
WHERE profit_rank <= 10
ORDER BY profit_rank;

-- -------------------------------------------------------------------
-- 4.7 Chart Top 10 products inside the 60–69.99% margin sub-band (core volume band in our data)
-- Ranking by total gross profit

SELECT
  '60–69.99%' AS margin_band,
  p.`Product Name`,
  ffc.`Product ID`,
  COUNT(*) AS num_records,
  ROUND(SUM(ffc.`Sales`), 2) AS sales,
  ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
  ROUND(AVG(ffc.gross_margin_pct), 2) AS avg_margin_pct
FROM v_financial_figures_clean ffc
JOIN `Product` p
  ON p.`Product ID` = ffc.`Product ID`
WHERE ffc.gross_margin_pct >= 60
  AND ffc.gross_margin_pct < 70
GROUP BY p.`Product Name`, ffc.`Product ID`
ORDER BY gross_profit DESC
LIMIT 10;

-- -------------------------------------------------------------------
-- 4.8 Chart Top 10 products inside the 70–79.99% margin band (high-margin engine)
-- Ranking by total gross profit

SELECT
  '70–79.99%' AS margin_band,
  p.`Product Name`,
  ffc.`Product ID`,
  COUNT(*) AS num_records,
  ROUND(SUM(ffc.`Sales`), 2) AS sales,
  ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
  ROUND(AVG(ffc.gross_margin_pct), 2) AS avg_margin_pct
FROM v_financial_figures_clean ffc
JOIN `Product` p
  ON p.`Product ID` = ffc.`Product ID`
WHERE ffc.gross_margin_pct >= 70
  AND ffc.gross_margin_pct < 80
GROUP BY p.`Product Name`, ffc.`Product ID`
ORDER BY gross_profit DESC
LIMIT 10;

-- --------------------------------------------------------------------------------------------------
-- 4.9 Chart Top 10 products per band (60–69.99% and 70–79.99%) in one output
-- This is cleaner for exporting and plotting.

WITH product_band AS (
  SELECT
    CASE
      WHEN ffc.gross_margin_pct >= 60 AND ffc.gross_margin_pct < 70 THEN '60–69.99%'
      WHEN ffc.gross_margin_pct >= 70 AND ffc.gross_margin_pct < 80 THEN '70–79.99%'
    END AS margin_band,

    p.`Product Name`,
    ffc.`Product ID`,

    -- Volume (number of transactions)
    COUNT(*) AS num_records,

    -- Revenue, Cost, and Profit aggregates
    SUM(ffc.`Sales`) AS sales_raw,
    SUM(ffc.`Cost`) AS cost_raw,
    SUM(ffc.`Gross Profit`) AS profit_raw,

    -- Average margin for stability check
    AVG(ffc.gross_margin_pct) AS avg_margin_raw

  FROM v_financial_figures_clean ffc
  JOIN `Product` p
    ON p.`Product ID` = ffc.`Product ID`

  WHERE ffc.gross_margin_pct >= 60
    AND ffc.gross_margin_pct < 80

  GROUP BY
    margin_band,
    p.`Product Name`,
    ffc.`Product ID`
),

ranked AS (
  SELECT
    margin_band,
    `Product Name`,
    `Product ID`,

    -- Volume
    num_records,

    -- Financials (rounded for reporting)
    ROUND(sales_raw, 2) AS sales,
    ROUND(cost_raw, 2) AS cost,
    ROUND(profit_raw, 2) AS gross_profit,
    ROUND(avg_margin_raw, 2) AS avg_margin_pct,

    -- Ranking within each margin band
    ROW_NUMBER() OVER (
      PARTITION BY margin_band
      ORDER BY profit_raw DESC
    ) AS rn

  FROM product_band
)

SELECT *
FROM ranked
WHERE rn <= 10
ORDER BY margin_band, rn;

-- ------------------------------------------------------------------------------------------
-- 4.10 Chart chart Low-margin products (<10%) —-----
-- For loss-prevention / cost-control lens.
-- ----------------------------------------------------------------------------------------
SELECT
  p.`Product Name`,
  ffc.`Product ID`,
  COUNT(*) AS num_records,
  ROUND(SUM(ffc.`Sales`), 2) AS sales,
  ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
  ROUND(100 * SUM(ffc.`Gross Profit`) / NULLIF(SUM(ffc.`Sales`), 0), 2) AS gross_margin_pct
FROM v_financial_figures_clean ffc
JOIN `Product` p ON p.`Product ID` = ffc.`Product ID`
WHERE ffc.gross_margin_pct < 10
GROUP BY p.`Product Name`, ffc.`Product ID`
ORDER BY sales DESC;

-- ------------------------------------------------------------------------------------------
-- 4.10 Chart Monitor -----------------------------------------------------------
-- To monitor products with extreme gross margins (>= 80%)
-- Not a "Top" ranking: it’s a flagged list for investigation.

SELECT 
    p.`Product Name`,
    ffc.`Product ID`,
    COUNT(*) AS num_records,
    ROUND(SUM(ffc.`Sales`), 2) AS sales,
    ROUND(SUM(ffc.`Cost`), 2) AS total_cost,
    ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
    ROUND(AVG(ffc.gross_margin_pct), 2) AS avg_margin_pct,
    ROUND(MIN(ffc.gross_margin_pct), 2) AS min_margin_pct,
    ROUND(MAX(ffc.gross_margin_pct), 2) AS max_margin_pct,
    ROUND(SUM(ffc.`Cost`) / NULLIF(COUNT(*), 0), 2) AS avg_cost_per_txn
FROM
    v_financial_figures_clean ffc
        JOIN
    `Product` p ON p.`Product ID` = ffc.`Product ID`
WHERE
    ffc.gross_margin_pct >= 80
        AND ffc.gross_margin_pct <= 100
GROUP BY p.`Product Name` , ffc.`Product ID`
ORDER BY avg_margin_pct DESC , num_records DESC;

-- ------------------------------------------------------------------------------------------
-- The next step is to replicate the same analytical lens for Factories -------------------
-- ------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------
-- 4.11 Chart Top 10 factories by Gross Profit within the high-margin universe (>=40% and <=100%)
-- Ranking metric: total gross profit (DESC)

WITH base AS (
  SELECT
    ffc.`Factory ID`,
    ffc.`Sales`,
    ffc.`Cost`,
    ffc.`Gross Profit`,
    ffc.gross_margin_pct
  FROM v_financial_figures_clean ffc
  WHERE ffc.gross_margin_pct >= 40
    AND ffc.gross_margin_pct <= 100
),
total_volume AS (
  -- Total transaction count in the >=40% margin universe (used to compute volume share)
  SELECT COUNT(*) AS total_transactions
  FROM base
),
factory_agg AS (
  SELECT
    b.`Factory ID`,
    COUNT(*) AS num_records,
    ROUND(100.0 * COUNT(*) / tv.total_transactions, 2) AS pct_transaction_volume,
    ROUND(SUM(b.`Cost`), 2) AS total_cost,
    ROUND(SUM(b.`Sales`), 2) AS total_sales,
    ROUND(SUM(b.`Gross Profit`), 2) AS gross_profit,
    ROUND(100 * SUM(b.`Gross Profit`) / NULLIF(SUM(b.`Sales`), 0), 2) AS gross_margin_pct
  FROM base b
  CROSS JOIN total_volume tv
  GROUP BY b.`Factory ID`, tv.total_transactions
),
ranked AS (
  SELECT
    *,
    DENSE_RANK() OVER (ORDER BY gross_profit DESC) AS profit_rank
  FROM factory_agg
)
SELECT *
FROM ranked
WHERE profit_rank <= 10
ORDER BY profit_rank;

-- ----------------------------------------------------------------------------
-- 4.12 Chart Top 10 factories per margin band (60–69.99% and 70–79.99%)
-- Ranking metric (within each band): total gross profit (DESC)

WITH base AS (
  SELECT
    CASE
      WHEN ffc.gross_margin_pct >= 60 AND ffc.gross_margin_pct < 70 THEN '60–69.99%'
      WHEN ffc.gross_margin_pct >= 70 AND ffc.gross_margin_pct < 80 THEN '70–79.99%'
    END AS margin_band,
    ffc.`Factory ID`,
    ffc.`Sales`,
    ffc.`Cost`,
    ffc.`Gross Profit`,
    ffc.gross_margin_pct
  FROM v_financial_figures_clean ffc
  WHERE ffc.gross_margin_pct >= 60
    AND ffc.gross_margin_pct < 80
),
factory_band_agg AS (
  SELECT
    margin_band,
    `Factory ID`,
    COUNT(*) AS num_records,
    ROUND(SUM(`Cost`), 2) AS total_cost,
    ROUND(SUM(`Sales`), 2) AS total_sales,
    ROUND(SUM(`Gross Profit`), 2) AS gross_profit,
    ROUND(AVG(gross_margin_pct), 2) AS avg_margin_pct
  FROM base
  GROUP BY margin_band, `Factory ID`
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY margin_band ORDER BY gross_profit DESC) AS rn
  FROM factory_band_agg
)
SELECT *
FROM ranked
WHERE rn <= 10
ORDER BY margin_band, rn;


-- ---------------------------------------------------------
-- 4.13 Chart Top 5 factories profit share (≥40%)
-- To see how dependent is total profit on just a handful of products

SELECT
  ffc.`Factory ID`,
  COUNT(*) AS num_records,
  ROUND(SUM(ffc.`Sales`), 2) AS sales,
  ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
  ROUND(100 * SUM(ffc.`Gross Profit`) / NULLIF(SUM(ffc.`Sales`), 0), 2) AS gross_margin_pct
FROM v_financial_figures_clean ffc
WHERE ffc.gross_margin_pct >= 40
GROUP BY ffc.`Factory ID`
ORDER BY gross_profit DESC;

-- ----------------------------------------------------------------------------
-- 4.14 Chart Monitor ----------------------------------------------------
-- To Monitor view for factories with extreme margins (>=80% and <=100%)
-- Not a "Top 10": this is a watchlist / anomaly surface.

SELECT
  ffc.`Factory ID`,
  COUNT(*) AS num_records,
  ROUND(SUM(ffc.`Cost`), 2) AS total_cost,
  ROUND(SUM(ffc.`Sales`), 2) AS total_sales,
  ROUND(SUM(ffc.`Gross Profit`), 2) AS gross_profit,
  ROUND(AVG(ffc.gross_margin_pct), 2) AS avg_margin_pct,
  ROUND(MIN(ffc.gross_margin_pct), 2) AS min_margin_pct,
  ROUND(MAX(ffc.gross_margin_pct), 2) AS max_margin_pct
FROM v_financial_figures_clean ffc
WHERE ffc.gross_margin_pct >= 80
  AND ffc.gross_margin_pct <= 100
GROUP BY ffc.`Factory ID`
ORDER BY avg_margin_pct DESC, num_records DESC;

-- ----------------------------------------------------------------------------
-- 4.15 Chart Cross product–factory dependency ------------------------------
-- -----------------------------------------------------------------------------
-- 1) Identify Top 10 products by total gross profit (within >=40% margin universe)
WITH top_products AS (
  SELECT
    ffc.`Product ID`,
    p.`Product Name`,
    ROUND(SUM(ffc.`Gross Profit`), 2) AS product_gross_profit
  FROM v_financial_figures_clean ffc
  JOIN `Product` p
    ON p.`Product ID` = ffc.`Product ID`
  WHERE ffc.gross_margin_pct >= 40
    AND ffc.gross_margin_pct <= 100
  GROUP BY ffc.`Product ID`, p.`Product Name`
  ORDER BY SUM(ffc.`Gross Profit`) DESC
  LIMIT 10
),

-- 2) For those top products, calculate factory contribution (profit + volume)
factory_contrib AS (
  SELECT
    tp.`Product ID`,
    tp.`Product Name`,
    ffc.`Factory ID`,
    COUNT(*) AS num_records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY tp.`Product ID`), 2) AS pct_product_volume_in_factory,
    ROUND(SUM(ffc.`Sales`), 2) AS total_sales,
    ROUND(SUM(ffc.`Cost`), 2) AS total_cost,
    ROUND(SUM(ffc.`Gross Profit`), 2) AS total_gross_profit,
    ROUND(100.0 * SUM(ffc.`Gross Profit`) / NULLIF(SUM(ffc.`Sales`), 0), 2) AS gross_margin_pct
  FROM v_financial_figures_clean ffc
  JOIN top_products tp
    ON tp.`Product ID` = ffc.`Product ID`
  WHERE ffc.gross_margin_pct >= 40
    AND ffc.gross_margin_pct <= 100
  GROUP BY tp.`Product ID`, tp.`Product Name`, ffc.`Factory ID`
)

-- 3) Output: per top product, which factories drive its profit + how concentrated it is
SELECT
  `Product Name`,
  `Product ID`,
  `Factory ID`,
  num_records,
  pct_product_volume_in_factory,
  total_cost,
  total_sales,
  total_gross_profit,
  gross_margin_pct
FROM factory_contrib
ORDER BY `Product Name`, total_gross_profit DESC;

-- ----------------------------------------------------------------------------
-- 4.16 Chart Margin stability over time (Factory margin band distribution by month)
-- Shows whether factories shift between margin bands across time.
-- ------------------------------------------------------------------
WITH base AS (
  SELECT
    ffc.`Factory ID`,
    o.`Order Date` AS order_date,
    ffc.gross_margin_pct,
    ffc.`Gross Profit`
  FROM v_financial_figures_clean ffc
  JOIN `Order` o
    ON o.`Order ID` = ffc.`Order ID`
  WHERE o.`Order Date` IS NOT NULL
),

banded AS (
  SELECT
    `Factory ID`,
    DATE_FORMAT(order_date, '%Y-%m-01') AS month_start,  -- monthly bucket
    CASE
      WHEN gross_margin_pct < 10 THEN '0–9.99%'
      WHEN gross_margin_pct < 20 THEN '10–19.99%'
      WHEN gross_margin_pct < 30 THEN '20–29.99%'
      WHEN gross_margin_pct < 40 THEN '30–39.99%'
      WHEN gross_margin_pct < 50 THEN '40–49.99%'
      WHEN gross_margin_pct < 60 THEN '50–59.99%'
      WHEN gross_margin_pct < 70 THEN '60–69.99%'
      WHEN gross_margin_pct < 80 THEN '70–79.99%'
      WHEN gross_margin_pct < 90 THEN '80–89.99%'
      ELSE '90–100%'
    END AS margin_band,
    `Gross Profit`
  FROM base
)

SELECT
  `Factory ID`,
  month_start,
  margin_band,
  COUNT(*) AS num_records,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY `Factory ID`, month_start), 2) AS pct_volume_in_month,
  ROUND(SUM(`Gross Profit`), 2) AS total_profit_in_band
FROM banded
GROUP BY `Factory ID`, month_start, margin_band
ORDER BY `Factory ID`, month_start, margin_band;

-- ----------------------------------------------------------------------------
-- 4.17 Chart Cost structure diagnostics (Factory efficiency / unit economics)
-- Compares cost per transaction and profit per transaction across factories.
-- -----------------------------------------------------------------------------
WITH base AS (
  SELECT
    ffc.`Factory ID`,
    ffc.`Sales`,
    ffc.`Cost`,
    ffc.`Gross Profit`,
    ffc.gross_margin_pct
  FROM v_financial_figures_clean ffc
  -- Optional: focus on the main profit universe
  WHERE ffc.gross_margin_pct >= 40
    AND ffc.gross_margin_pct <= 100
)

SELECT
  `Factory ID`,
  COUNT(*) AS num_records,

  -- Totals
  ROUND(SUM(`Cost`), 2)  AS total_cost,
  ROUND(SUM(`Sales`), 2) AS total_sales,
  ROUND(SUM(`Gross Profit`), 2) AS total_gross_profit,

  -- Unit economics per transaction (operational vs pricing signal)
  ROUND(AVG(`Cost`), 2)  AS avg_cost_per_txn,
  ROUND(AVG(`Sales`), 2) AS avg_sales_per_txn,
  ROUND(AVG(`Gross Profit`), 2) AS avg_profit_per_txn,

  -- Margin (pricing power signal, aggregated)
  ROUND(100.0 * SUM(`Gross Profit`) / NULLIF(SUM(`Sales`), 0), 2) AS gross_margin_pct

FROM base
GROUP BY `Factory ID`
ORDER BY avg_profit_per_txn DESC, gross_margin_pct DESC;

-- ----------------------------------------------------------------------------









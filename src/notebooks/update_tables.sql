-- Databricks notebook source
-- Enzyme Demo: Incremental Data Changes (Scenarios 1-4)
-- Target: ~250 rows for incremental refresh demonstration

USE CATALOG ${catalog};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

-- SCENARIO 1: Aggregation over JOIN (mv_regional_customers)
-- Insert 100 sales for 5 specific customers
-- Affects: 5 customer aggregates (order_count, total_spend)
INSERT INTO sales
SELECT
    500000000 + id AS sale_id,
    (id % 5) + 1 AS customer_id,
    (id % 100) + 1 AS product_id,
    CAST((id % 5) + 1 AS INT) AS quantity,
    CAST(100.00 AS DECIMAL(10, 2)) AS unit_price,
    CAST(10.00 AS DECIMAL(5, 2)) AS discount_percent,
    'Online' AS channel,
    'Completed' AS status,
    CURRENT_DATE() AS sale_date
FROM RANGE(1, 101);

-- COMMAND ----------

-- SCENARIO 2: Window Functions (mv_customer_ltv)
-- Update 20 sales to 'Returned' for 2 customers
-- Affects: Window aggregates for customers 1 and 2 only
UPDATE sales
SET status = 'Returned'
WHERE sale_id BETWEEN 500000001 AND 500000020;

-- COMMAND ----------

-- SCENARIO 3: Composable Aggregation (mv_category_perf)
-- Insert 50 sales for Electronics category only
-- Affects: 1 category aggregate (Electronics)
INSERT INTO sales
SELECT
    500000100 + id AS sale_id,
    (id * 1234 % 10000000) + 1 AS customer_id,
    (id * 10) AS product_id,
    CAST((id % 8) + 1 AS INT) AS quantity,
    CAST(200.00 AS DECIMAL(10, 2)) AS unit_price,
    CAST(5.00 AS DECIMAL(5, 2)) AS discount_percent,
    'Store' AS channel,
    'Completed' AS status,
    CURRENT_DATE() AS sale_date
FROM RANGE(1, 51);

-- COMMAND ----------

-- SCENARIO 4: Temporal Filters (mv_daily_sales)
-- Insert 100 completed sales for today
-- Affects: 1 group (today + Online + Electronics)
INSERT INTO sales
SELECT
    500000150 + id AS sale_id,
    (id * 5678 % 10000000) + 1 AS customer_id,
    (id * 10) AS product_id,
    CAST((id % 6) + 1 AS INT) AS quantity,
    CAST(150.00 AS DECIMAL(10, 2)) AS unit_price,
    CAST(8.00 AS DECIMAL(5, 2)) AS discount_percent,
    'Online' AS channel,
    'Delivered' AS status,
    CURRENT_DATE() AS sale_date
FROM RANGE(1, 101);

-- COMMAND ----------

-- Verify changes (should be ~250 new rows)
SELECT 'New rows inserted' AS operation, COUNT(*) AS count
FROM sales WHERE sale_id >= 500000000;

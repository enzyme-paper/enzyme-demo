-- Databricks notebook source
-- Enzyme Demo: Input Data Creation
-- Dates spanning 1 year
-- Uses partitioning for optimal query performance

USE CATALOG ${catalog};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

DROP TABLE IF EXISTS sales;

-- COMMAND ----------

DROP TABLE IF EXISTS customers;

-- COMMAND ----------

DROP TABLE IF EXISTS products;

-- COMMAND ----------

-- Dimension: Customers (10M records)
-- Partitioned by region for regional analytics
CREATE TABLE customers
PARTITIONED BY (region)
TBLPROPERTIES (delta.enableRowTracking = true)
AS SELECT
    id AS customer_id,
    CONCAT('Customer_', id) AS customer_name,
    CASE (id % 5)
        WHEN 0 THEN 'Gold'
        WHEN 1 THEN 'Silver'
        WHEN 2 THEN 'Bronze'
        WHEN 3 THEN 'Platinum'
        ELSE 'Standard'
    END AS membership_tier,
    DATE_SUB(CURRENT_DATE(), CAST(id % 1000 AS INT)) AS signup_date,
    CAST((id * 17 % 100) + 18 AS INT) AS age,
    ARRAY('North', 'South', 'East', 'West', 'Central')[id % 5] AS region
FROM RANGE(1, 10000001);

-- COMMAND ----------

-- Dimension: Products (1M records)
-- Partitioned by category for category-based queries
CREATE TABLE products
PARTITIONED BY (category)
TBLPROPERTIES (delta.enableRowTracking = true)
AS SELECT
    id AS product_id,
    CONCAT('Product_', id) AS product_name,
    ARRAY('BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE')[id % 5] AS brand,
    CAST((id * 13 % 1000) + 10 AS DECIMAL(10, 2)) AS unit_price,
    CAST((id * 7 % 1000) + 50 AS INT) AS stock_quantity,
    ARRAY('Electronics', 'Clothing', 'Home', 'Sports', 'Books',
          'Food', 'Beauty', 'Toys', 'Garden', 'Auto')[id % 10] AS category
FROM RANGE(1, 1000001);

-- COMMAND ----------

-- Fact: Sales (500M records)
-- Partitioned by sale_date for time-series queries
CREATE TABLE sales
PARTITIONED BY (sale_date)
TBLPROPERTIES (delta.enableRowTracking = true)
AS SELECT
    id AS sale_id,
    (id % 10000000) + 1 AS customer_id,
    (id % 1000000) + 1 AS product_id,
    CAST((id * 3 % 10) + 1 AS INT) AS quantity,
    CAST((id * 11 % 500) + 50 AS DECIMAL(10, 2)) AS unit_price,
    CAST((id * 7 % 30) AS DECIMAL(5, 2)) AS discount_percent,
    ARRAY('Online', 'Store', 'Mobile', 'Partner')[id % 4] AS channel,
    ARRAY('Pending', 'Completed', 'Shipped', 'Delivered', 'Returned')[id % 5] AS status,
    DATE_SUB(CURRENT_DATE(), CAST(id % 365 AS INT)) AS sale_date
FROM RANGE(1, 500000001);

-- COMMAND ----------

-- Verify table sizes
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'sales', COUNT(*) FROM sales;

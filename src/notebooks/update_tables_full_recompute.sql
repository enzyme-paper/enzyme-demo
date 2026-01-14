-- Databricks notebook source
-- Data Quality Backfill: Correcting historical pricing errors
-- A bug caused 10% of sales to record incorrect unit prices
-- This backfill corrects the prices based on the product catalog

USE CATALOG ${catalog};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

UPDATE sales
SET unit_price = (SELECT MAX(p.unit_price) * 0.9 FROM products p WHERE p.product_id = sales.product_id)
WHERE sale_id % 10 = 0;

# Enzyme Demo: Incremental View Maintenance for Data Engineering

End-to-end demo of [Enzyme](https://www.databricks.com/blog/enzyme-incremental-view-maintenance), the incremental view maintenance engine powering Databricks serverless pipelines. Uses a Databricks Asset Bundle (DAB) to deploy and orchestrate everything.

## What the Demo Does

The demo creates an e-commerce dataset (10M customers, 1M products, 500M sales) and runs a [Spark Declarative Pipeline](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines) with 4 materialized views that showcase different Enzyme microtechniques:

| MV | Pattern | Description |
|----|---------|-------------|
| `mv_regional_customers` | Aggregation over LEFT JOIN | Regional customer spend analysis |
| `mv_customer_ltv` | Window functions | Customer lifetime value with cumulative spend |
| `mv_category_perf` | Composable aggregation (agg over agg) | Category performance metrics |
| `mv_daily_sales` | Temporal filter + JOIN + aggregation | Daily sales summary (last 90 days) |

The orchestration job runs 6 steps:

1. **Create source tables** — customers, products, sales
2. **Full refresh** — initial materialization of all MVs
3. **Small incremental update** — ~250 row changes across 4 scenarios
4. **Incremental refresh** — Enzyme processes only the changed data
5. **Bulk update** — correct 10% of all sales prices (simulates a backfill)
6. **Cost-model recompute** — Enzyme detects the bulk change and triggers full recompute

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.200+) with bundle support
- A Databricks workspace with Unity Catalog enabled — [sign up for the free edition](https://www.databricks.com/try-databricks) if you don't have one
- A SQL warehouse (the free edition includes a starter warehouse)

## Project Structure

```
├── databricks.yml                  # DAB configuration (bundle, variables, resources, targets)
├── src/
│   ├── mv_transformations.sql      # Pipeline: 4 materialized view definitions
│   └── notebooks/
│       ├── create_tables.sql       # Creates source tables (customers, products, sales)
│       ├── update_tables.sql       # Small incremental changes (~250 rows)
│       └── update_tables_full_recompute.sql  # Bulk price correction (10% of sales)
```

## Setup

### 1. Register a Databricks Workspace

If you don't already have a workspace, sign up at [databricks.com/try-databricks](https://www.databricks.com/try-databricks). The free edition includes everything needed for this demo.

### 2. Authenticate

Log in with the Databricks CLI:

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

### 3. Configure

The bundle uses three variables defined in `databricks.yml`:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Unity Catalog name | `main` |
| `schema` | Schema for tables and pipeline output | `enzyme_demo` |
| `warehouse_id` | SQL warehouse ID for notebook tasks | (none) |

Set your workspace host and warehouse ID as environment variables:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_WAREHOUSE_ID=your_warehouse_id
```

You can find your warehouse ID in the Databricks UI under **SQL Warehouses** — it's the hex string in the warehouse URL.

### 4. Validate

Check that the bundle configuration is correct:

```bash
databricks bundle validate
```

### 5. Deploy

Deploy the bundle resources (pipeline, job, schema) to the workspace:

```bash
databricks bundle deploy
```

This creates:
- A Unity Catalog schema (`main.enzyme_demo` by default)
- A declarative pipeline (`enzyme-demo-mv-transformations`)
- A workflow job (`enzyme-demo-orchestration`) with the 6-step task chain

### 6. Run the Demo

Run the orchestration job end-to-end:

```bash
databricks bundle run enzyme_demo_job
```

This triggers the full demo sequence (create tables -> full refresh -> incremental update -> incremental refresh -> bulk update -> cost-model recompute). The job takes several minutes depending on warehouse size.

You can also run just the pipeline independently:

```bash
databricks bundle run mv_transformations             # incremental refresh
databricks bundle run mv_transformations --refresh-all  # full refresh
```

### 7. Monitor

After running, you can monitor progress in the Databricks workspace UI:
- **Workflows** tab: shows the job run with all 6 task steps
- **Pipelines** tab: shows the pipeline with MV lineage and refresh details

### 8. Tear Down

To remove all deployed resources:

```bash
databricks bundle destroy
```

## Iterating on the Demo

If you modify the SQL files or `databricks.yml`, redeploy with:

```bash
databricks bundle deploy && databricks bundle run enzyme_demo_job
```

To add or modify materialized views, edit `src/mv_transformations.sql`. The pipeline will pick up changes on the next deploy + run.

# Sales ETL Pipeline with Databricks

## Overview

This project implements an ETL pipeline on Databricks to process sales data from a CSV source, transform and aggregate business metrics, and load the results into final tables for analytics.

The pipeline is orchestrated using Databricks Workflows and follows a three-stage architecture:

```text
Extract -> Transform -> Load
```

## Project Structure

```text
sales-etl-pipeline/
│
├── notebooks/
│   ├── extract.py
│   ├── transform_aggregate.py
│   └── load.py
│
├── resources/
│   └── etl_job.yml
│
└── README.md
```

## Pipeline Steps

### 1. Extract

* Read raw sales data from a CSV file.
* Infer schema automatically.
* Store raw data in the staging layer.

Output:

```text
dbacademy.etl_staging.sales_raw
```

### 2. Transform & Aggregate

Data processing includes:

* Convert `order_date` to date format.
* Create revenue column (`quantity × unit_price`).
* Extract year, month, and quarter.
* Filter completed orders.
* Generate aggregated business reports.

Output tables:

```text
sales_enriched
sales_by_region
sales_by_category
monthly_sales
top10_products
```

### 3. Load

Load transformed datasets from the staging layer into the final schema for reporting and analysis.

Output schema:

```text
dbacademy.default
```

## Workflow

The ETL process is scheduled and managed using Databricks Workflows.

Task dependency:

```text
Extract
   ↓
Transform
   ↓
Load
```


# Databricks notebook source
# 3_load - Copy dữ liệu từ staging sang final tables (dbacademy.default)

staging_schema = "dbacademy.etl_staging"
final_schema = "dbacademy.default"

tables = [
    "sales_enriched",
    "sales_by_region",
    "sales_by_category",
    "monthly_sales",
    "top10_products"
]

# COMMAND ----------


for table in tables:
    staging_table = f"{staging_schema}.{table}"
    final_table = f"{final_schema}.{table}"

    df = spark.table(staging_table)
    df.write.mode("overwrite").saveAsTable(final_table)

    print(f"{table} completed")

print("Load finished")

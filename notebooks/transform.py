# Databricks notebook source
# Databricks notebook source
# Transform dữ liệu bán hàng và tạo các bảng tổng hợp

from pyspark.sql.functions import col, to_date, year, month, quarter, sum as _sum, count, round, desc
source_raw = "dbacademy.etl_staging.sales_raw"
target_enriched = "dbacademy.etl_staging.sales_enriched"
target_region = "dbacademy.etl_staging.sales_by_region"
target_category = "dbacademy.etl_staging.sales_by_category"
target_monthly = "dbacademy.etl_staging.monthly_sales"
target_top10 = "dbacademy.etl_staging.top10_products"

# COMMAND ----------

df_raw = spark.table(source_raw)

# Chuẩn hóa định dạng ngày
df_clean = df_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# COMMAND ----------

# Tính doanh thu cho từng đơn hàng
df_enriched = df_clean.withColumn(
    "revenue",
    round(col("quantity") * col("unit_price"), 2)
)

# COMMAND ----------

# Tách thông tin thời gian phục vụ báo cáo
df_enriched = df_enriched.withColumn("year", year("order_date")) \
                         .withColumn("month", month("order_date")) \
                         .withColumn("quarter", quarter("order_date"))


# COMMAND ----------

# Chỉ lấy các đơn hàng hoàn thành
df_completed = df_enriched.filter(col("status") == "completed")

df_enriched.write.mode("overwrite").saveAsTable(target_enriched)

print("Transform completed")


# COMMAND ----------

# Tổng hợp theo khu vực
sales_by_region = df_completed.groupBy("region").agg(
    round(_sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("num_orders")
).orderBy(desc("total_revenue"))

sales_by_region.write.mode("overwrite").saveAsTable(target_region)


# COMMAND ----------

# Tổng hợp theo danh mục sản phẩm
sales_by_category = df_completed.groupBy("category").agg(
    round(_sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("num_orders")
).orderBy(desc("total_revenue"))

sales_by_category.write.mode("overwrite").saveAsTable(target_category)


# COMMAND ----------

# Doanh thu theo tháng
monthly_sales = df_completed.groupBy("year", "month").agg(
    round(_sum("revenue"), 2).alias("total_revenue")
).orderBy("year", "month")

monthly_sales.write.mode("overwrite").saveAsTable(target_monthly)


# COMMAND ----------

# Top 10 sản phẩm doanh thu cao nhất
top_products = df_completed.groupBy("product_id", "product_name").agg(
    round(_sum("revenue"), 2).alias("total_revenue"),
    _sum("quantity").alias("total_quantity")
).orderBy(desc("total_revenue")).limit(10)

top_products.write.mode("overwrite").saveAsTable(target_top10)

print("Aggregation completed")
# Databricks notebook source

# Đọc file CSV chứa dữ liệu bán hàng năm 2024
# Sau đó lưu dữ liệu thô vào bảng staging để phục vụ các bước xử lý tiếp theo
input_path = "/Volumes/dbacademy/default/tutorials/sales_2024.csv"
staging_table = "dbacademy.etl_staging.sales_raw"

# COMMAND ----------

print(f"Đọc dữ liệu từ: {input_path}")

df_raw = spark.read.format("csv") \
                 .option("header", "true") \
                 .option("inferSchema", "true") \
                 .load(input_path)

row_count = df_raw.count()
print(f"Số bản ghi: {row_count}")
print(f"Số cột: {len(df_raw.columns)}")

# COMMAND ----------

# Ghi đè dữ liệu vào bảng staging mỗi khi notebook được chạy
df_raw.write.mode("overwrite").saveAsTable(staging_table)

print(f"Hoàn tất nạp dữ liệu vào bảng {staging_table}")
# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://raw@datafactorylab01hwr.blob.core.windows.net",
    mount_point = "/mnt/raw",
    extra_configs = {"fs.azure.account.key.datafactorylab01hwr.blob.core.windows.net":"ZVP0spIp6GoNBrdiQcUwAtNUlCbg5QwyQ7QB/l+y3Vn2JnztZkVCUyTYAkWBW0jSvpzppUJn6l7v+ASt3rkfAA=="} 
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw/")

# COMMAND ----------

df = spark.read.format("parquet").options(header='True', inferSchema='True').load('dbfs:/mnt/raw/dbo.pizza_sales.parquet')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM pizza_sales_analysis;

# COMMAND ----------

#We want to track:
# 1. Total pizzas sold
# 2. Total orders, total revenue, avg pizza per orders
# 3. Sales vs order month over month
# 4. Daily orders, monthly, hourly order trends
# 5. Sales by pizza category
# 6. Sales by pizza size
# 7. Top 5 orders pizza
# we need to build all of this into power BI

# So we need, the day of the week, month

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pizza_sale_fact_2 AS SELECT COUNT(DISTINCT order_id) order_id, sum(quantity) quantity, date_format(order_date, 'MMM') month_name, date_format(order_date, 'EEEE') day_name, hour(order_time) hour,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_sales, pizza_size, pizza_category, pizza_name FROM pizza_sales_analysis GROUP BY order_id, order_date, order_time, pizza_size, pizza_category, pizza_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pizza_sale_fact_2;

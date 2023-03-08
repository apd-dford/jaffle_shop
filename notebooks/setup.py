# Databricks notebook source
# MAGIC %md
# MAGIC # Using dbt and databricks Auto Loader to transform streaming data
# MAGIC 
# MAGIC Jaffle Shop has grown rapidly and now wants to track sales at an hourly level throughout the day and still use dbt for data transformations! Thanks to databricks, that's actually pretty straightforward!

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# DBTITLE 1,Check you can access the files from the repo
# MAGIC %sh ls /Workspace/Repos/<username>/jaffle_shop/seeds 
# MAGIC 
# MAGIC # you should see a list of 11 files, 1 customer, 5 orders and 5 payments files from the jaffle_shop repo

# COMMAND ----------

# DBTITLE 1,Copy customers static file
customers_file = "raw_customers.csv"
dbutils.fs.cp(f"file:/Workspace/Repos/{username}/jaffle_shop/seeds/{customers_file}", f"{target_path}/customers/{customers_file}")

# COMMAND ----------

# DBTITLE 1,Load data to the static jaffle_shop_customers_raw table
(spark.read
  .option("header", True)
  .csv(f"{target_path}/customers/")
  .write
  .mode("overwrite")
  .saveAsTable(f"{catalog_name}.{database_name}.jaffle_shop_customers_raw")
 )

# COMMAND ----------

# MAGIC %sql select * from main.default.jaffle_shop_customers_raw limit 5

# COMMAND ----------

# DBTITLE 1,Function to delete data, tables and views
# *** WARNING ***
# below function removes ALL data from the <target_path> directory

def cleanup():
    tables = ("customers", "payments", "orders")
    for table in tables:
        sql(f"drop table if exists {catalog_name}.{database_name}.jaffle_shop_{table}_raw")
        sql(f"drop view if exists {catalog_name}.{database_name}.stg_{table}")
        sql(f"drop table if exists {catalog_name}.{database_name}.{table}")
    dbutils.fs.rm(f"{target_path}/", True)

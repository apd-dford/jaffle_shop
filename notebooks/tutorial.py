# Databricks notebook source
# MAGIC %md
# MAGIC # Using dbt and databricks Auto Loader to transform streaming data
# MAGIC 
# MAGIC Jaffle Shop has grown rapidly and now wants to track sales at an hourly level throughout the day, but still use dbt for data transformations! Thanks to databricks, that's actually pretty straightforward. In this guide, you will:
# MAGIC 
# MAGIC 1. Clone a fork of the dbt Jaffle Shop repo - updated with the sample data changed to 5 hourly files of orders and payments and minimal changes to show how dbt can be used on databricks Auto Loader structured streaming
# MAGIC 2. create functions to simulate loading those hourly files, by copying one hour at a time into dbfs, where a structured streaming job will recognize new files and update our raw jaffle_shop.orders and jaffle_shop.payments source data
# MAGIC 3. dbt will then read from the "live" raw data tables and basically do it's thing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone the Repo
# MAGIC 
# MAGIC do these steps to clone the repo to databricks

# COMMAND ----------

# DBTITLE 1,Your username is used in a couple places
repo_username = "dylan.ford@aimpointdigital.com"
# run `dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()` to check your username

target_path = "dbfs:/test/jaffle_shop/"

# COMMAND ----------

# DBTITLE 1,Clone the Repo
# MAGIC %md
# MAGIC 
# MAGIC Navigate to the Repos tab in your databricks, clone this fork of the jaffle_shop repo and pull the streamify branch, which provides the hourly files
# MAGIC 
# MAGIC https://github.com/apd-dford/jaffle_shop

# COMMAND ----------

# DBTITLE 1,Check you can access the files from the repo
# MAGIC %sh ls /Workspace/Repos/dylan.ford@aimpointdigital.com/jaffle_shop/seeds 
# MAGIC 
# MAGIC # you should see a list of 11 files, 1 customer, 5 orders and 5 payments files from the jaffle_shop repo

# COMMAND ----------

# DBTITLE 1,Copy customers static file
customers_file = "raw_customers.csv"
dbutils.fs.cp(f"file:/Workspace/Repos/{repo_username}/jaffle_shop/seeds/{customers_file}", f"{target_path}/customers/{customers_file}")

# COMMAND ----------

# DBTITLE 1,Load data to the static jaffle_shop_customers_raw table
(spark.read
  .option("header", True)
  .csv(f"{target_path}/customers/")
  .write
  .mode("overwrite")
  .saveAsTable("main.default.jaffle_shop_customers_raw")
 )

# COMMAND ----------

# MAGIC %sql select * from main.default.jaffle_shop_customers_raw limit 5

# COMMAND ----------

# DBTITLE 1,This function moves a set of files from the repo onto dbfs, where we will have autoloader setup
def simulate_data_feed(hour: int):
    orders_file = f"raw_orders_2018_01_01_{hour}.csv"
    payments_file = f"raw_payments_2018_01_01_{hour}.csv"
    dbutils.fs.cp(f"file:/Workspace/Repos/{repo_username}/jaffle_shop/seeds/{orders_file}", f"{target_path}/orders/{orders_file}")
    dbutils.fs.cp(f"file:/Workspace/Repos/{repo_username}/jaffle_shop/seeds/{payments_file}", f"{target_path}/payments/{payments_file}")

# COMMAND ----------

# DBTITLE 1,Run structured streaming function to read all files in directly and update the streaming table
def process_orders(file_type: str):  
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", f"{target_path}/schemas/{file_type}_schema")
                  .load(f"{target_path}/{file_type}/")
                  .writeStream
                  .option("checkpointLocation", f"{target_path}/schemas/{file_type}/")
                  .trigger(availableNow=True)
                  .table(f"main.default.jaffle_shop_{file_type}_raw")
            )
 
    query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Function to delete data and raw tables
def cleanup():
    tables = ("customers", "payments", "orders")
    sql(f"drop table main.default.jaffle_shop_{table}_raw") for table in tables
    dbutils.fs.rm(f"{target_path}/", True)

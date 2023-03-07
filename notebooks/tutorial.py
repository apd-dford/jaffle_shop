# Databricks notebook source
# MAGIC %md
# MAGIC # Using dbt and databricks Auto Loader to transform streaming data
# MAGIC 
# MAGIC Jaffle Shop has grown rapidly and now wants to track sales at an hourly level throughout the day and still use dbt for data transformations! Thanks to databricks, that's actually pretty straightforward. In this guide, you will:
# MAGIC 
# MAGIC 1. Clone a fork of the dbt Jaffle Shop repo into your databricks workspace. The repo has been updated with the sample data changed to 5 hourly files of orders and payments and minimal changes to show how dbt can be used on databricks Auto Loader structured streaming
# MAGIC 2. create a function to simulate loading those hourly files, by copying one hour at a time into dbfs and pausing for a few minutes
# MAGIC 3. creating a databricks job that uses auto loader to pick up any files in dbfs and load them into streaming tables - then run the dbt transformation to create jaffle shop orders table

# COMMAND ----------

# DBTITLE 0,Clone the Repo
# MAGIC %md
# MAGIC 
# MAGIC 1. Navigate to the Repos tab in your databricks workspace
# MAGIC 2. Clone the below repo (which is a fork of the jaffle_shop repo)
# MAGIC 3. Switch to the streamify branch, then pull it
# MAGIC 
# MAGIC https://github.com/apd-dford/jaffle_shop

# COMMAND ----------

repo_username = "<your_username>"
# run `dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()` to check your username

target_path = "dbfs:/test/jaffle_shop/" # update to wherever you like, I chose dbfs for simplicity. In a real scenario this is the location where you are getting live data

# COMMAND ----------

# DBTITLE 1,Check you can access the files from the repo
# MAGIC %sh ls /Workspace/Repos/<your_username/jaffle_shop/seeds 
# MAGIC 
# MAGIC # you should see a list of 11 files, 1 customer, 5 orders and 5 payments files from the jaffle_shop repo

# COMMAND ----------

# DBTITLE 1,Copy customers static file
customers_file = "raw_customers.csv"
dbutils.fs.cp(f"file:/Workspace/Repos/{repo_username}/jaffle_shop/seeds/{customers_file}", f"{target_path}/customers/{customers_file}")

# COMMAND ----------

# DBTITLE 1,Load data to the static jaffle_shop_customers_raw table
catalog_name = "main"
database_name = "default"

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
                  .table(f"{catalog_name}.{database_name}.jaffle_shop_{file_type}_raw")
            )
 
    query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Function to delete data and raw tables
def cleanup():
    tables = ("customers", "payments", "orders")
    sql(f"drop table {catalog_name}.{database_name}.jaffle_shop_{table}_raw") for table in tables
    dbutils.fs.rm(f"{target_path}/", True)

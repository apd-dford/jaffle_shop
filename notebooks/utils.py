# Databricks notebook source
# MAGIC %run ./variables

# COMMAND ----------

# DBTITLE 0,This function moves a set of files from the repo onto dbfs, where we will have autoloader setup
def simulate_data_feed(hour: int):
    orders_file = f"raw_orders_2018_01_01_{hour}.csv"
    payments_file = f"raw_payments_2018_01_01_{hour}.csv"
    dbutils.fs.cp(f"file:/Workspace/Repos/{username}/jaffle_shop/seeds/{orders_file}", f"{target_path}/orders/{orders_file}")
    dbutils.fs.cp(f"file:/Workspace/Repos/{username}/jaffle_shop/seeds/{payments_file}", f"{target_path}/payments/{payments_file}")

# COMMAND ----------

# DBTITLE 1,Run structured streaming function to read all files in directly and update the streaming table
def process_data(file_type: str):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", f"{target_path}/schemas/{file_type}_schema")
                  .load(f"{target_path}/{file_type}/")
                  .writeStream
                  .option("checkpointLocation", f"{target_path}/checkpoints/{file_type}/")
                  .trigger(processingTime='30 seconds')
                  .table(f"{catalog_name}.{database_name}.jaffle_shop_{file_type}_raw")
            )
 
    query.awaitTermination()

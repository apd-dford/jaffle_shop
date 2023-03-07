# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

import time
repo_username = "<your_username>"
target_path = "dbfs:/test/jaffle_shop/"

# COMMAND ----------

def slowly_add_data():
    for hour in range(10,15):
        simulate_data_feed(hour)
        time.sleep(120)
        print(f"Successfully copied hour {hour} file to {target_path}")
# COMMAND ----------

slowly_add_data()

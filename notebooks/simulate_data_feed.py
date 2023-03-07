# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

import time

# COMMAND ----------

def slowly_add_data():
    for hour in range(10,15):
        simulate_data_feed(hour)
        print(f"Successfully copied hour {hour} file to {target_path}")
        time.sleep(120)
        

# COMMAND ----------

slowly_add_data()

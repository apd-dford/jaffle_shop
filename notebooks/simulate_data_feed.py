# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

import time

# COMMAND ----------

def slowly_add_data():
    for i in range(10,15):
        simulate_data_feed(i)
        time.sleep(120)

# COMMAND ----------

slowly_add_data()

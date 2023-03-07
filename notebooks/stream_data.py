# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

import time

# COMMAND ----------

simulate_data_feed(10)

# COMMAND ----------

# MAGIC %fs ls /test/jaffle_shop/

# COMMAND ----------

time.sleep(3)

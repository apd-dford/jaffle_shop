# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

repo_username = "<your_username>"
target_path = "dbfs:/test/jaffle_shop/"

# COMMAND ----------

process_data("orders")

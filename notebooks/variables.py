# Databricks notebook source
databricks_username = "<your_username>"
# run `dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()` to check your username

catalog_name = "main"
database_name = "default"
target_path = "dbfs:/test/jaffle_shop/"
seconds_between_files = 120

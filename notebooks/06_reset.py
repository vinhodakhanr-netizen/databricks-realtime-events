# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Reset (start from zero)
# MAGIC Drops every table and deletes landing files and checkpoints. Use it when you want to replay the
# MAGIC whole project. **This deletes data.**

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

for t in ["bronze_events", "silver_events", "gold_events_per_minute", "gold_revenue_by_product", "gold_funnel"]:
    spark.sql(f"DROP TABLE IF EXISTS {table(t)}")

for p in [f"{LANDING}/events", f"{CHECKPOINTS}/bronze_events", f"{CHECKPOINTS}/silver_events", f"{CHECKPOINTS}/gold"]:
    dbutils.fs.rm(p, True)

print("Reset done. Run 01_event_generator to start again.")

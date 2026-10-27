# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver stream
# MAGIC Reads new rows from the bronze table (a Delta table can be a streaming source), cleans them with
# MAGIC `clean_events()` from the repo's `src/transforms.py`, removes duplicates, and appends to silver.
# MAGIC
# MAGIC Key ideas:
# MAGIC - **Watermark**: how late data may arrive; lets Spark drop old duplicate-tracking state.
# MAGIC - The same function is unit-tested on GitHub, so the logic is trusted before it runs here.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from src.transforms import clean_events

bronze_stream = spark.readStream.table(table("bronze_events"))

query = (clean_events(bronze_stream, watermark="1 hour")
         .writeStream
         .option("checkpointLocation", f"{CHECKPOINTS}/silver_events")
         .trigger(availableNow=True)
         .toTable(table("silver_events")))

query.awaitTermination()

# COMMAND ----------

bronze = spark.table(table("bronze_events")).count()
silver = spark.table(table("silver_events")).count()
print(f"bronze={bronze}  silver={silver}  removed so far={bronze - silver}")
display(spark.table(table("silver_events")).limit(10))

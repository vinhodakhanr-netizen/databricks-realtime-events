# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze stream (Auto Loader)
# MAGIC Reads **only new files** from the landing volume and appends them, unchanged, to a Delta table.
# MAGIC
# MAGIC Key ideas:
# MAGIC - **Auto Loader (`cloudFiles`)** remembers which files it has already seen.
# MAGIC - **Checkpoint** = the saved progress. Run again and it continues where it stopped.
# MAGIC - **`trigger(availableNow=True)`** = process everything new, then stop (the mode serverless supports).

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA_DDL = ("event_id STRING, user_id STRING, event_type STRING, product_id STRING, "
              "price STRING, device STRING, event_ts STRING")

raw = (spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .schema(SCHEMA_DDL)
       .load(f"{LANDING}/events")
       .withColumn("_source_file", F.col("_metadata.file_path"))
       .withColumn("_ingest_ts", F.current_timestamp()))

query = (raw.writeStream
         .option("checkpointLocation", f"{CHECKPOINTS}/bronze_events")
         .trigger(availableNow=True)
         .toTable(table("bronze_events")))

query.awaitTermination()

# COMMAND ----------

print("bronze_events rows:", spark.table(table("bronze_events")).count())
display(spark.table(table("bronze_events")).orderBy(F.desc("_ingest_ts")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Try it:** run this notebook twice in a row. The second run should add 0 rows, because no new
# MAGIC files arrived. Then run `01_event_generator` and run this one again: only the new files are read.

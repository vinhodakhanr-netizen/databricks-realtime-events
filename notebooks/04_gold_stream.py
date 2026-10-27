# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold stream (foreachBatch)
# MAGIC Every time new silver rows arrive, rebuild the small reporting tables.
# MAGIC `foreachBatch` lets you run any normal PySpark code for each micro-batch.
# MAGIC (For big data you would update incrementally with MERGE; recomputing is fine at training scale.)

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from src.transforms import events_per_minute, revenue_by_product, conversion_funnel


def refresh_gold(batch_df, batch_id):
    silver = spark.table(table("silver_events"))
    events_per_minute(silver).write.mode("overwrite").saveAsTable(table("gold_events_per_minute"))
    revenue_by_product(silver).write.mode("overwrite").saveAsTable(table("gold_revenue_by_product"))
    conversion_funnel(silver).write.mode("overwrite").saveAsTable(table("gold_funnel"))
    print(f"batch {batch_id}: gold tables refreshed")


query = (spark.readStream.table(table("silver_events"))
         .writeStream
         .foreachBatch(refresh_gold)
         .option("checkpointLocation", f"{CHECKPOINTS}/gold")
         .trigger(availableNow=True)
         .start())

query.awaitTermination()

# COMMAND ----------

display(spark.table(table("gold_events_per_minute")))
display(spark.table(table("gold_revenue_by_product")).limit(10))
display(spark.table(table("gold_funnel")))

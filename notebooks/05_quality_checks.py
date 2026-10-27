# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data quality checks
# MAGIC Fails the run if something is wrong, so a scheduled Job turns red instead of silently
# MAGIC producing bad numbers.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

bronze = spark.table(table("bronze_events"))
silver = spark.table(table("silver_events"))

results = []


def check(name, ok, detail=""):
    results.append((name, "PASS" if ok else "FAIL", detail))


n_b, n_s = bronze.count(), silver.count()
check("silver has rows", n_s > 0, f"silver={n_s}")
check("silver <= bronze", n_s <= n_b, f"bronze={n_b}, silver={n_s}")

dupes = silver.groupBy("event_id").count().filter("count > 1").count()
check("silver event_id unique", dupes == 0, f"duplicate ids={dupes}")

bad = silver.filter("user_id IS NULL OR price < 0 OR event_ts IS NULL").count()
check("silver has no bad rows", bad == 0, f"bad rows={bad}")

funnel = {r.event_type: r.users for r in spark.table(table("gold_funnel")).collect()}
check("funnel: views >= purchases", funnel.get("page_view", 0) >= funnel.get("purchase", 0), str(funnel))

# COMMAND ----------

display(spark.createDataFrame(results, "check STRING, status STRING, detail STRING"))
failed = [r[0] for r in results if r[1] == "FAIL"]
assert not failed, f"Failed checks: {failed}"
print("All checks passed")

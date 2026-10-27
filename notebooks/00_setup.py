# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the schema and two volumes, and makes the repo's `src/` folder importable.
# MAGIC Other notebooks load this with `%run ./00_setup`.

# COMMAND ----------

import os
import sys

CATALOG = "workspace"            # default catalog in Databricks Free Edition
SCHEMA = "realtime_events"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"          # new event files arrive here
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"  # streaming progress is stored here


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# Make `from src.transforms import ...` work: the repo root is the parent of the notebooks folder
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

print("Landing     :", LANDING)
print("Checkpoints :", CHECKPOINTS)
print("Repo root   :", repo_root)

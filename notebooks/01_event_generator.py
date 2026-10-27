# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Event generator (the "live" source)
# MAGIC Simulates a website sending clickstream events. Every few seconds it drops a new small
# MAGIC JSON file into the landing volume, like a real app, Kafka sink or IoT device would.
# MAGIC
# MAGIC Some events are deliberately bad (missing user, negative price, duplicates, messy text).
# MAGIC Run this notebook as many times as you like: every run adds NEW files.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

import json
import os
import random
import time
import uuid
import datetime as dt

FILES = 6              # how many files to drop in this run
EVENTS_PER_FILE = 200
SLEEP_SECONDS = 5      # pause between files (the "live" feel)

out_dir = f"{LANDING}/events"
os.makedirs(out_dir, exist_ok=True)

price_rng = random.Random(1)   # fixed product prices, so every run agrees
prices = {pid: round(price_rng.uniform(5, 300), 2) for pid in range(1, 51)}

event_types = ["page_view"] * 6 + ["add_to_cart"] * 2 + ["purchase", "remove_from_cart"]
devices = ["mobile", "desktop", "tablet", "MOBILE"]

previous_rows = []

# COMMAND ----------

for i in range(FILES):
    now = dt.datetime.now()
    rows = []
    for _ in range(EVENTS_PER_FILE):
        pid = random.randint(1, 50)
        ts = now - dt.timedelta(seconds=random.randint(0, 20))
        row = {
            "event_id": str(uuid.uuid4()),
            "user_id": str(random.randint(1, 300)),
            "event_type": random.choice(event_types),
            "product_id": str(pid),
            "price": str(prices[pid]),
            "device": random.choice(devices),
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
        r = random.random()
        if r < 0.01:
            row["user_id"] = None                  # missing user
        elif r < 0.02:
            row["price"] = "-5.0"                  # negative price
        elif r < 0.03:
            row["event_type"] = "  Purchase "      # messy but fixable
        rows.append(row)

    if previous_rows:
        rows += random.sample(previous_rows, 5)    # duplicates arriving again (at-least-once delivery)
    previous_rows = rows[:EVENTS_PER_FILE]

    path = f"{out_dir}/events_{now:%Y%m%d_%H%M%S}_{i}.json"
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {os.path.basename(path)} ({len(rows)} events)")

    if i < FILES - 1:
        time.sleep(SLEEP_SECONDS)

# COMMAND ----------

print("Files in landing zone:", len(os.listdir(out_dir)))

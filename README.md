# Near-Real-Time Clickstream Pipeline (PySpark + Databricks + GitHub)

A training project that shows how a **streaming data pipeline** and a **GitHub workflow** fit together,
the way they do on a real data engineering team. All data is simulated, so it runs on Databricks Free Edition.

## What "real time" means here

A website sends events (page views, add-to-cart, purchases) all day. Instead of waiting for a nightly
batch, the pipeline processes **new files as they arrive** using Spark Structured Streaming + Auto Loader.

Honest limit: Free Edition only has serverless compute, which supports `trigger(availableNow=True)`
(process everything new, then stop) and not an always-on stream. So you get **near real time**: a Job
runs the pipeline every few minutes and each run reads only what is new. This is a common production pattern too.

```
 event_generator ──► landing volume (JSON files arriving)
                          │  Auto Loader (reads only NEW files, remembers progress in a checkpoint)
                          ▼
                   bronze_events   raw copy + audit columns
                          │  clean_events()  type, validate, dedupe (watermark)
                          ▼
                   silver_events
                          │  foreachBatch -> refresh reporting tables
                          ▼
     gold_events_per_minute · gold_revenue_by_product · gold_funnel
```

## Repo layout

```
notebooks/            Databricks notebooks (source format .py)
  00_setup.py           schema, volumes, config, makes src/ importable
  01_event_generator.py drops new JSON event files (the "live" source)
  02_bronze_stream.py   Auto Loader -> bronze_events
  03_silver_stream.py   clean + dedupe -> silver_events
  04_gold_stream.py     foreachBatch -> gold tables
  05_quality_checks.py  assertions (fails the run if data is wrong)
  06_reset.py           wipe everything and start over
src/transforms.py     pure functions, no I/O: the testable business logic
tests/                pytest unit tests (run on GitHub, no Databricks needed)
.github/workflows/ci.yml   GitHub Actions: lint + tests on every push / pull request
```

## Streaming ideas you will practise

| Idea | Where you see it |
|------|------------------|
| Source / sink | Auto Loader in, Delta table out |
| Checkpoint | `CHECKPOINTS` volume: saved progress, restart-safe |
| Trigger | `availableNow=True` |
| Watermark + dedupe | `clean_events()` |
| foreachBatch | `04_gold_stream.py` |
| At-least-once delivery | generator re-sends duplicates, silver removes them |

## Part A: Put the project on GitHub

1. Create a free GitHub account and a **new repository** named `databricks-realtime-events`. Leave
   "Add a README" **unticked** so the repo starts empty.
2. Unzip this project on your computer. On the repo page click **uploading an existing file**, drag in
   everything (including the hidden `.github` folder) and click **Commit changes**.
   (If the `.github` folder is skipped by the browser, use **Add file → Create new file**, type
   `.github/workflows/ci.yml` as the name, and paste the contents.)
3. Click the **Actions** tab. A run called **CI** should start; wait for the green tick. This is
   GitHub testing your code automatically.

## Part B: Connect GitHub to Databricks

The screens change over time, so look for the closest wording if a label differs.

1. **GitHub token:** GitHub → profile picture → **Settings → Developer settings → Personal access
   tokens → Fine-grained tokens → Generate new token**. Give it access to your repo only, with
   **Contents: Read and write**. Copy the token (GitHub shows it once).
2. **Link it in Databricks:** click your profile picture (top right) → **Settings → Linked accounts**
   (or Git integration), choose **GitHub**, **Personal access token**, paste it, save.
3. **Create the Git folder:** **Workspace → Create → Git folder**, paste your repo URL, choose GitHub,
   click **Create**. Your notebooks now appear in Databricks, and `src/` can be imported by them.

## Part C: Run the pipeline

1. Open `notebooks/00_setup`, connect **Serverless**, **Run all**.
2. Run **01_event_generator** (about 30 seconds). Open Catalog → workspace → realtime_events → landing to see the files.
3. Run **02_bronze_stream**, **03_silver_stream**, **04_gold_stream**, **05_quality_checks** in order.
4. **Feel the streaming:** run 02 again right away. It adds 0 rows (nothing new). Run 01 again, then 02, 03, 04:
   only the new files are processed and the gold numbers grow.

If `from src.transforms import ...` fails, the repo root is not on the path. Check that you opened the
notebooks from the Git folder and that `00_setup` printed a "Repo root" that contains the `src` folder.

## Part D: Run it automatically (near real time)

1. **Jobs & Pipelines → Create → Job**, name it `clickstream_pipeline`.
2. Add tasks in this order, each depending on the one before, all on Serverless:
   `generate` (01) → `bronze` (02) → `silver` (03) → `gold` (04) → `checks` (05).
   Pick each notebook from your Git folder.
3. **Run now**, then add a **schedule** (for example every 10 minutes). Each run drops new events and
   processes them. Free Edition has usage limits, so switch the schedule off when you finish practising.

## Part E: The daily GitHub workflow (the real-world habit)

```
Databricks Git folder ──branch──► edit notebook ──commit & push──► GitHub branch
                                                                       │ open Pull Request
                                                           CI runs flake8 + pytest
                                                                       │ green? merge to main
Databricks Git folder ◄──── Pull ◄─────────────────────────────────────┘
```

1. In the Git folder, click the branch name → **Create branch** `feature/add-device-metric`.
2. Change code (for example add a function in `src/transforms.py` and a test for it).
3. Click the **Git** button → write a message → **Commit & Push**.
4. On GitHub click **Compare & pull request → Create pull request**. Wait for the CI tick.
5. Click **Merge pull request**. Back in Databricks switch to `main` and click **Pull**.
6. Never edit `main` directly. A red CI cross means a test failed: read the log, fix, push again.

## Run the tests on your own computer (optional)

Needs Python 3.10+ and Java 17.
```
pip install -r requirements-dev.txt
pytest -q
```

## Exercises

1. Add a gold table `gold_device_split` (events by device) with a unit test, via a pull request.
2. Send bad rows to a `silver_events_rejected` table with a reason, using `foreachBatch` in silver.
3. Add `gold_sessions`: a 30-minute session window per user (`F.session_window`).
4. Add a CI check that fails if a notebook still has a hard-coded catalog name other than `CATALOG`.
5. Replace the generator with a second source (orders CSV) and join it in gold.
6. Switch gold from "recompute" to an incremental `MERGE INTO`.

# 📅 Airflow 3.0+ Scheduling, Catchup, and DAG Timing

## Overview
In Apache Airflow 3.0 and later, scheduling behavior is more predictable but slightly stricter.  
This guide explains how `schedule`, `catchup`, and `max_active_runs` work together — and how to design DAGs that behave correctly when running daily or performing backfills.

---

## 1️⃣  The `@dag()` Schedule Parameters

### Example
```python
@dag(
    dag_id="mongo_to_snowflake_dag",
    start_date=datetime(2024, 10, 20),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "mongo", "snowflake"]
)
```

| Parameter | Purpose |
|------------|----------|
| `start_date` | The first logical execution date for the DAG. |
| `schedule` | Defines how often Airflow should create runs (`@daily`, `@weekly`, or cron string). |
| `catchup` | If `True`, Airflow creates *all* missed runs since `start_date`; if `False`, only the next run is triggered. |
| `max_active_runs` | Limits how many DAG runs can execute at once (important for catch-up). |

---

## 2️⃣  Daily Scheduling

Setting  
```python
schedule="@daily"
catchup=False
```
means the DAG will run **once per day at midnight (UTC)** starting from the next day after `start_date`.  
Each run represents a *logical day* of data.

---

## 3️⃣  Understanding Catch-Up

When `catchup=True`, Airflow will:
1. Look at `start_date` and today’s date.
2. Create one DAG Run for each day between them.
3. Run them sequentially or in limited parallel depending on `max_active_runs`.

### Example
If your DAG’s `start_date` is **2024-10-20** and today is **2025-10-20**,  
Airflow will queue ~365 runs — one per day — until it “catches up.”

Each run’s logical data window:
```
2024-10-20 → 2024-10-21
2024-10-21 → 2024-10-22
...
```

---

## 4️⃣  Controlling Backfill Speed: `max_active_runs`

| Setting | Effect |
|----------|--------|
| `max_active_runs=1` | One DAG Run executes at a time (safest). |
| `max_active_runs=5` | Up to 5 days run concurrently. |
| `max_active_runs=7` | Seven backfill days run in parallel until all catch up. |

Example:

```
@dag(
    ...,
    catchup=True,
    max_active_runs=7
)
```

This lets the scheduler keep 7 daily runs active at once, starting new ones as old ones finish — like a sliding window through time.

---

## 5️⃣  When *Not* to Use Catch-Up

If your pipeline:
- always performs a **full refresh** (re-loads all data each run), or  
- doesn’t parameterize by execution date (`{{ ds }}`)

then `catchup=True` will simply repeat the same work many times.  
Keep `catchup=False` for these DAGs.

Use `catchup=True` only when your extract logic filters data by **execution date**, for example:

```python
@task()
def extract_from_mongo(data_interval_start=None, data_interval_end=None):
    query = {
        "date": {
            "$gte": data_interval_start.date().isoformat(),
            "$lt": data_interval_end.date().isoformat()
        }
    }
    docs = list(collection.find(query, {"_id": 0}))
```

This ensures each historical run loads only its day’s slice of data — safe and idempotent.

---

## 6️⃣  Summary Table

| Goal | Recommended Settings |
|------|-----------------------|
| Daily full refresh | `@daily`, `catchup=False`, `max_active_runs=1` |
| Daily incremental load | `@daily`, `catchup=True`, `max_active_runs=3–7` |
| Initial backfill of a year | Temporarily set `catchup=True`, tune `max_active_runs` for parallel speed, then disable afterward |

---

## 7️⃣  Key Takeaways for ITM 327 Students
- **Airflow 3.0+** moved many DAG-level options (`owner`, `retries`) to task level.  
- `catchup=True` makes Airflow generate *past* runs automatically.  
- `max_active_runs` throttles concurrency — Airflow will “slide” through time until caught up.  
- Always make ETLs **date-aware** if you intend to backfill historical data.

---

**Author:** ITM 327 — Data Engineering Fundamentals  
**Topic:** Airflow 3.0+ DAG Scheduling, Catch-Up, and Backfill Behavior  
**Last Updated:** October 2025  

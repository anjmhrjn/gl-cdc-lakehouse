"""Measurements for the tuning experiments and the cost figures in docs/results.md.

  databricks bundle run tuning_probe -t dev --params mode=stats
  databricks bundle run tuning_probe -t dev --params mode=queries,label=baseline
  databricks bundle run tuning_probe -t dev --params mode=history
  databricks bundle run tuning_probe -t dev --params mode=cost
  databricks bundle run tuning_probe -t dev --params mode=skew

Modes:
  stats    file count and size of every streaming table, and the duration of each flow
           in the most recent pipeline updates (event log). DESCRIBE DETAIL rejects
           the gold materialized views as views, so they are left out.
  queries  runs the benchmark queries over silver.journal_entries and prints the label
           with each result
  history  bytes and files read, files pruned and duration of every benchmark query
           this job has run, from system.query.history. Records can take up to an hour
           to appear there.
  cost     DBUs and list price per pipeline update and per job run, from
           system.billing.usage joined to system.billing.list_prices
  skew     times the gold join of journal lines to accounts three ways: forced
           shuffle (sort-merge), Spark's own choice, and forced broadcast. The hot
           account holds ~30% of lines, so a shuffle join sends ~30% of the rows to one
           task.

It only reads, so it can run as often as needed. It runs as the job owner, who must be
able to read the event log and the system tables.
"""

import argparse
import sys
import time

TABLES = (
    "bronze.accounts_cdc_raw",
    "bronze.journal_entries_cdc_raw",
    "silver.accounts",
    "silver.account_history",
    "silver.journal_entries",
    "silver.quarantine_events",
)

UPDATES_SHOWN = 5

BENCHMARKS = ("cold_account_day", "cold_account", "hot_account_day", "one_day")


def show(df, n: int = 100) -> None:
    df.show(n, truncate=False)


def stats(spark, catalog: str, pipeline_id: str) -> None:
    for table in TABLES:
        row = spark.sql(f"DESCRIBE DETAIL `{catalog}`.{table}").first()
        print(
            f"{table}: files={row['numFiles']} bytes={row['sizeInBytes']}"
            f" clustering={row['clusteringColumns']}"
        )

    # One row per flow per update: when it started and finished, and rows written.
    show(
        spark.sql(
            f"""
            WITH updates AS (
              SELECT origin.update_id, min(timestamp) AS started
              FROM event_log('{pipeline_id}')
              GROUP BY origin.update_id
              ORDER BY started DESC
              LIMIT {UPDATES_SHOWN}
            )
            SELECT u.started AS update_started, e.origin.update_id,
                   e.origin.flow_name,
                   round((unix_millis(max(e.timestamp)) - unix_millis(min(e.timestamp)))
                         / 1000, 1) AS flow_seconds,
                   max(e.details:flow_progress.metrics.num_output_rows::bigint)
                     AS output_rows
            FROM event_log('{pipeline_id}') e
            JOIN updates u ON e.origin.update_id = u.update_id
            WHERE e.event_type = 'flow_progress'
            GROUP BY ALL
            ORDER BY update_started DESC, flow_seconds DESC
            """
        ),
        200,
    )
    show(
        spark.sql(
            f"""
            SELECT origin.update_id, min(timestamp) AS started, max(timestamp) AS ended,
                   round((unix_millis(max(timestamp)) - unix_millis(min(timestamp)))
                         / 1000, 1) AS update_seconds
            FROM event_log('{pipeline_id}')
            WHERE origin.update_id IS NOT NULL
            GROUP BY origin.update_id
            ORDER BY started DESC
            LIMIT {UPDATES_SHOWN}
            """
        )
    )


def queries(spark, catalog: str, label: str) -> None:
    je = f"`{catalog}`.silver.journal_entries"
    # The hot account holds ~30% of lines; a cold one is a typical account. Chosen from
    # the data so the same query shape works on any dataset.
    counts = spark.sql(
        f"SELECT account_id, count(*) AS n FROM {je} GROUP BY account_id ORDER BY n DESC"
    ).collect()
    hot = counts[0]["account_id"]
    cold = counts[len(counts) // 2]["account_id"]
    day = spark.sql(f"SELECT max(entry_date) AS d FROM {je}").first()["d"]
    print(f"hot={hot} cold={cold} day={day}")

    benchmarks = {
        "cold_account_day": f"account_id = '{cold}' AND entry_date = DATE'{day}'",
        "cold_account": f"account_id = '{cold}'",
        "hot_account_day": f"account_id = '{hot}' AND entry_date = DATE'{day}'",
        "one_day": f"entry_date = DATE'{day}'",
    }
    assert list(benchmarks) == list(BENCHMARKS)
    for name, where in benchmarks.items():
        sql = f"SELECT count(*) AS n, sum(amount) AS total FROM {je} WHERE {where}"
        # history mode finds these by this exact source line; see its docstring.
        row = spark.sql(sql).first()
        print(f"{label} {name}: rows={row['n']} total={row['total']}")


def history(spark, job_id: str) -> None:
    """Files and bytes read by each benchmark query, one row per query per run.

    For a serverless Python job, query history stores the Python source line that ran
    the query, not the SQL. So the benchmark queries are found by this job's id and the
    line `row = spark.sql(sql).first()`, and named by their order within the run.
    """
    names = ", ".join(f"{i + 1}, '{name}'" for i, name in enumerate(BENCHMARKS))
    show(
        spark.sql(
            f"""
            SELECT min(start_time) OVER (PARTITION BY query_source.job_info.job_run_id)
                     AS run_started,
                   element_at(map({names}), row_number() OVER (
                     PARTITION BY query_source.job_info.job_run_id ORDER BY start_time
                   )) AS query,
                   read_bytes, read_files, pruned_files, total_duration_ms
            FROM system.query.history
            WHERE query_source.job_info.job_id = '{job_id}'
              AND statement_text LIKE '%row = spark.sql(sql).first()%'
              AND start_time >= current_timestamp() - INTERVAL 7 DAYS
            ORDER BY run_started, query
            """
        ),
        500,
    )


def cost(spark, pipeline_id: str) -> None:
    priced = """
        SELECT u.*, u.usage_quantity * p.pricing.effective_list.default AS list_usd
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name
         AND u.cloud = p.cloud
         AND u.usage_unit = p.usage_unit
         AND u.usage_end_time >= p.price_start_time
         AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
        WHERE u.usage_date >= current_date() - INTERVAL 14 DAYS
    """
    show(
        spark.sql(
            f"""
            SELECT usage_metadata.dlt_update_id AS update_id,
                   min(usage_start_time) AS first_usage,
                   round(sum(usage_quantity), 3) AS dbus,
                   round(sum(list_usd), 3) AS list_usd
            FROM ({priced})
            WHERE usage_metadata.dlt_pipeline_id = '{pipeline_id}'
            GROUP BY ALL
            ORDER BY first_usage DESC
            """
        ),
        50,
    )
    show(
        spark.sql(
            f"""
            SELECT usage_metadata.job_id, usage_metadata.job_run_id,
                   min(usage_start_time) AS first_usage,
                   round(sum(usage_quantity), 3) AS dbus,
                   round(sum(list_usd), 3) AS list_usd
            FROM ({priced})
            WHERE usage_metadata.job_id IS NOT NULL
            GROUP BY ALL
            ORDER BY first_usage DESC
            """
        ),
        50,
    )


SKEW_REPEATS = 3


def skew(spark, catalog: str, src: str) -> None:
    sys.path.insert(0, src)
    from pyspark.sql import functions as F

    from transforms import gold

    je = spark.read.table(f"`{catalog}`.silver.journal_entries")
    history = spark.read.table(f"`{catalog}`.silver.account_history")
    share = je.groupBy("account_id").count().orderBy(F.desc("count")).first()
    print(f"hottest account {share['account_id']}: {share['count']} of {je.count()} lines")

    strategies = {"shuffle": "merge", "default": None, "broadcast": "broadcast"}
    for name, hint in strategies.items():
        accounts = gold.latest_accounts(history)
        if hint:
            accounts = accounts.hint(hint)
        # The same aggregate as gold.daily_trial_balance, with the join strategy forced.
        df = (
            je.join(accounts, "account_id", "left")
            .groupBy("business_unit", "currency", "entry_date")
            .agg(F.count(F.lit(1)).alias("n"), F.sum("amount").alias("total"))
        )
        seconds = []
        for _ in range(SKEW_REPEATS):
            start = time.monotonic()
            rows = df.collect()
            seconds.append(round(time.monotonic() - start, 2))
        print(f"{name}: groups={len(rows)} seconds={seconds} median={sorted(seconds)[1]}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--pipeline-id", required=True)
    p.add_argument(
        "--mode", required=True, choices=["stats", "queries", "history", "cost", "skew"]
    )
    p.add_argument("--label", default="unlabelled")
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    p.add_argument("--job-id", required=True, help="this job's id, for history mode")
    args = p.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    if args.mode == "stats":
        stats(spark, args.catalog, args.pipeline_id)
    elif args.mode == "queries":
        queries(spark, args.catalog, args.label)
    elif args.mode == "history":
        history(spark, args.job_id)
    elif args.mode == "skew":
        skew(spark, args.catalog, args.src)
    else:
        cost(spark, args.pipeline_id)


if __name__ == "__main__":
    main()

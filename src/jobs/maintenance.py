"""OPTIMIZE and VACUUM the pipeline's streaming tables, on demand.

  databricks bundle run maintenance -t dev
  databricks bundle run maintenance -t dev --params tables=silver.journal_entries,vacuum=false

Predictive optimization is enabled on gl_dev and gl_prod (inherited from the
metastore), and it already runs OPTIMIZE and VACUUM on these tables on its own
schedule. So this job has no schedule. It is for when waiting is not acceptable: after
a large backfill, and for the compaction experiment in docs/results.md, which needs
compaction to happen at a known moment between two measurements.

Only streaming tables are listed. The gold materialized views are small aggregates that
every update rewrites.

For each table it prints the file count and size before and after, from DESCRIBE
DETAIL. VACUUM keeps the default 7 day retention, so time travel and running readers
are unaffected.
"""

import argparse

TABLES = (
    "bronze.accounts_cdc_raw",
    "bronze.journal_entries_cdc_raw",
    "silver.accounts",
    "silver.account_history",
    "silver.journal_entries",
    "silver.quarantine_events",
)


def detail(spark, name: str) -> str:
    row = spark.sql(f"DESCRIBE DETAIL {name}").first()
    return (
        f"files={row['numFiles']} bytes={row['sizeInBytes']}"
        f" clustering={row['clusteringColumns']}"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--tables", default="", help="comma separated schema.table; empty for all")
    p.add_argument("--vacuum", default="true", choices=["true", "false"])
    args = p.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or list(TABLES)
    unknown = sorted(set(tables) - set(TABLES))
    if unknown:
        raise SystemExit(f"not a pipeline streaming table: {', '.join(unknown)}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for table in tables:
        name = f"`{args.catalog}`.{table}"
        print(f"{table} before: {detail(spark, name)}")
        spark.sql(f"OPTIMIZE {name}")
        if args.vacuum == "true":
            spark.sql(f"VACUUM {name}")
        print(f"{table} after:  {detail(spark, name)}")


if __name__ == "__main__":
    main()

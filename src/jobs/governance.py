"""Applies sql/governance/*.sql to one catalog, file by file in name order.

Runs as the `governance` bundle job on serverless compute, so no SQL warehouse is
involved:

  databricks bundle run governance -t dev

The SQL files name objects as schema.table and never name a catalog. This job sets the
catalog once with USE CATALOG, so the same files serve dev and prod. To run a file by
hand in the SQL editor, run `USE CATALOG gl_dev;` first.

Every statement is CREATE OR REPLACE, ALTER or GRANT, so a re-run converges on the same
state. The pipeline has to have created the tables before this runs.
"""

import argparse
from pathlib import Path


def statements(text: str) -> list[str]:
    """Split a SQL file into statements.

    Full-line `--` comments are dropped first, so a comment may contain a semicolon.
    Statements must not, which holds for everything in sql/governance.
    """
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--sql-dir", required=True, help="workspace path of sql/governance")
    args = p.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")

    for path in sorted(Path(args.sql_dir).glob("*.sql")):
        stmts = statements(path.read_text())
        print(f"{path.name}: {len(stmts)} statements")
        for stmt in stmts:
            print(f"  {stmt.splitlines()[0]}")
            spark.sql(stmt)


if __name__ == "__main__":
    main()

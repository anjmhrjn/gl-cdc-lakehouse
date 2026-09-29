"""Tags, masks, row filters and grants in one catalog match what checkpoint 4 requires.

Runs as the `governance_check` bundle job on serverless compute:

  databricks bundle run governance_check -t dev

It reads information_schema, not sql/governance, so a statement missing from the SQL
files shows up here as a failure. It checks what is attached, not what anyone sees. What
each group sees is checked by hand as a test user, with governance_toggle.sql.

Run it after the governance job, and again after a pipeline update. If it passes the
second time, the masks and filters survived the refresh.
"""

import argparse

from pyspark.sql import SparkSession

CLASSIFICATION = {
    ("bronze", "accounts_cdc_raw"): "confidential",
    ("bronze", "journal_entries_cdc_raw"): "confidential",
    ("silver", "accounts"): "confidential",
    ("silver", "account_history"): "confidential",
    ("silver", "journal_entries"): "confidential",
    ("silver", "quarantine_events"): "confidential",
    ("gold", "account_balances"): "internal",
    ("gold", "daily_trial_balance"): "internal",
}

# (schema, table, column) -> mask function name. Every masked column is also a pii column.
MASKS = {
    ("silver", "accounts", "account_number"): "mask_account_number",
    ("silver", "accounts", "holder_name"): "mask_pii",
    ("silver", "accounts", "holder_email"): "mask_pii",
    ("silver", "account_history", "account_number"): "mask_account_number",
    ("silver", "account_history", "holder_name"): "mask_pii",
    ("silver", "account_history", "holder_email"): "mask_pii",
    ("bronze", "accounts_cdc_raw", "before"): "mask_account_row",
    ("bronze", "accounts_cdc_raw", "after"): "mask_account_row",
    ("bronze", "accounts_cdc_raw", "_corrupt_record"): "mask_pii",
    ("bronze", "accounts_cdc_raw", "_rescued_data"): "mask_pii",
    ("silver", "quarantine_events", "payload"): "mask_payload",
}

ROW_FILTERED = [
    ("silver", "accounts"),
    ("silver", "account_history"),
    ("gold", "account_balances"),
    ("gold", "daily_trial_balance"),
]


def unquote(name: str | None) -> list[str]:
    """`a`.`b` or a.b -> ["a", "b"]."""
    return [part.strip("` ") for part in name.split(".")] if name else []


def is_function(name: str | None, catalog: str, want: str) -> bool:
    """The views have no column for the function's schema, and the name may or may not be
    qualified. Accept bare, schema qualified, or fully qualified in this catalog's silver.
    """
    return unquote(name) in ([want], ["silver", want], [catalog, "silver", want])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    info = f"`{args.catalog}`.information_schema"
    problems = []

    table_tags = {
        (r.schema_name, r.table_name): r.tag_value
        for r in spark.sql(
            f"SELECT schema_name, table_name, tag_value FROM {info}.table_tags"
            " WHERE tag_name = 'classification'"
        ).collect()
    }
    for key, want in CLASSIFICATION.items():
        got = table_tags.get(key)
        if got != want:
            problems.append(f"classification on {'.'.join(key)}: {got}, want {want}")

    # Every silver and gold table must be classified, including ones added after this list.
    # The pipeline's own backing tables and event log are skipped: they sit in the same
    # schemas, but Unity Catalog passes no schema grant down to them (see ARCHITECTURE.md).
    for r in spark.sql(
        f"SELECT table_schema, table_name FROM {info}.tables"
        " WHERE table_schema IN ('silver', 'gold')"
        " AND NOT startswith(table_name, '__materialization_')"
        " AND NOT startswith(table_name, 'event_log_')"
    ).collect():
        if (r.table_schema, r.table_name) not in table_tags:
            problems.append(f"no classification on {r.table_schema}.{r.table_name}")

    pii = {
        (r.schema_name, r.table_name, r.column_name)
        for r in spark.sql(
            f"SELECT schema_name, table_name, column_name FROM {info}.column_tags"
            " WHERE tag_name = 'pii' AND tag_value = 'true'"
        ).collect()
    }
    for key in MASKS.keys() - pii:
        problems.append(f"no pii tag on {'.'.join(key)}")

    # Column names here were read from DESCRIBE on the views, not from the docs. See
    # AI_USAGE.md.
    masks = {
        (r.table_schema, r.table_name, r.column_name): r.mask_name
        for r in spark.sql(
            f"SELECT table_schema, table_name, column_name, mask_name FROM {info}.column_masks"
        ).collect()
    }
    for key, want in MASKS.items():
        got = masks.get(key)
        if not is_function(got, args.catalog, want):
            problems.append(f"mask on {'.'.join(key)}: {got}, want silver.{want}")

    filters = {
        (r.table_schema, r.table_name): (r.filter_name, r.target_columns)
        for r in spark.sql(
            f"SELECT table_schema, table_name, filter_name, target_columns FROM {info}.row_filters"
        ).collect()
    }
    for key in ROW_FILTERED:
        name, columns = filters.get(key, (None, None))
        on_unit = unquote(columns) == ["business_unit"]
        if not (is_function(name, args.catalog, "bu_filter") and on_unit):
            problems.append(f"row filter on {'.'.join(key)}: {name} on {columns}")

    # No principal holds ALL PRIVILEGES on the catalog or any of its schemas.
    for view in ("catalog_privileges", "schema_privileges"):
        for r in spark.sql(
            f"SELECT grantee FROM {info}.{view} WHERE privilege_type = 'ALL PRIVILEGES'"
        ).collect():
            problems.append(f"ALL PRIVILEGES granted to {r.grantee} ({view})")

    print(
        f"checked {len(CLASSIFICATION)} classifications, {len(MASKS)} masks and pii tags,"
        f" {len(ROW_FILTERED)} row filters, ALL PRIVILEGES grants"
    )
    for problem in problems:
        print(f"  FAIL {problem}")
    if problems:
        raise SystemExit(f"{len(problems)} governance problems in {args.catalog}")
    print(f"governance in {args.catalog} is as required")


if __name__ == "__main__":
    main()

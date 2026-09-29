"""Silver: AUTO CDC from bronze, keyed on the primary key and sequenced by source.lsn.

  accounts         SCD1, deletes applied
  account_history  SCD2, a delete closes the current version
  journal_entries  SCD1, deletes applied

Only the rows AUTO CDC cannot key or sequence are dropped here (missing PK, null lsn,
unknown op). Quarantine rules come in checkpoint 3.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transforms import cdc

CATALOG = spark.conf.get("gl.catalog")

# AUTO CDC keeps a deleted key as a tombstone so a late, older event for that key is
# recognised as stale instead of re-inserting the row. The default is two days, which is
# exactly the generator's maximum lateness (48h) with no room for the gap between
# landing and the next scheduled run, or for a backfill. Seven days covers both.
TOMBSTONE_RETENTION_SECONDS = str(7 * 24 * 3600)


def define_events_view(table: str) -> None:
    @dp.temporary_view(name=f"{table}_events")
    @dp.expect_all_or_drop(cdc.drop_rules(table))
    def events():
        raw = spark.readStream.table(f"{CATALOG}.bronze.{table}_cdc_raw")
        return cdc.dedupe(cdc.flatten(raw, table), table)


def define_cdc_target(table: str, target: str, scd_type: int, properties: dict) -> None:
    name = f"{CATALOG}.silver.{target}"
    dp.create_streaming_table(name=name, table_properties=properties)
    dp.create_auto_cdc_flow(
        target=name,
        source=f"{table}_events",
        keys=[cdc.TABLES[table]["pk"]],
        sequence_by=F.col("_lsn"),
        apply_as_deletes=F.expr("_op = 'd'"),
        except_column_list=cdc.META_COLUMNS,
        stored_as_scd_type=scd_type,
    )


tombstones = {"pipelines.cdc.tombstoneGCThresholdInSeconds": TOMBSTONE_RETENTION_SECONDS}

for _table in cdc.TABLES:
    define_events_view(_table)

define_cdc_target("accounts", "accounts", 1, tombstones)
define_cdc_target("accounts", "account_history", 2, {})
define_cdc_target("journal_entries", "journal_entries", 1, tombstones)

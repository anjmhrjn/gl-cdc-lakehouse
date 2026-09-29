"""Silver: AUTO CDC from bronze, keyed on the primary key and sequenced by source.lsn.

  accounts           SCD1, deletes applied
  account_history    SCD2, a delete closes the current version
  journal_entries    SCD1, deletes applied
  quarantine_events  append-only, rows kept out of the three above

Per source table, each event goes one way:

  <table>_parsed    flattened bronze rows, unparseable ones included
    unparseable  -> quarantine_events
  <table>_checked   parseable, drop rules applied (missing PK, null lsn, unknown op),
                    deduplicated, quarantine reasons computed
    any reason   -> quarantine_events
  <table>_valid     no reason -> AUTO CDC

Unparseable rows are split off before the drop rules on purpose: they have no key, so
the missing-PK rule would drop them and they would never reach quarantine.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transforms import cdc

CATALOG = spark.conf.get("gl.catalog")
QUARANTINE = f"{CATALOG}.silver.quarantine_events"

# AUTO CDC keeps a deleted key as a tombstone so a late, older event for that key is
# recognised as stale instead of re-inserting the row. The default is two days, which is
# exactly the generator's maximum lateness (48h) with no room for the gap between
# landing and the next scheduled run, or for a backfill. Seven days covers both.
TOMBSTONE_RETENTION_SECONDS = str(7 * 24 * 3600)


def define_events(table: str) -> None:
    @dp.temporary_view(name=f"{table}_parsed")
    def parsed():
        return cdc.flatten(spark.readStream.table(f"{CATALOG}.bronze.{table}_cdc_raw"), table)

    # Warn-only copies of the quarantine rules put their failure counts in the event
    # log next to the drop counts. The rows themselves are routed below.
    @dp.temporary_view(name=f"{table}_checked")
    @dp.expect_all_or_drop(cdc.drop_rules(table))
    @dp.expect_all(cdc.quarantine_rules(table))
    def checked():
        return cdc.check(spark.readStream.table(f"{table}_parsed"), table)

    @dp.temporary_view(name=f"{table}_valid")
    def valid():
        return cdc.valid(spark.readStream.table(f"{table}_checked"), table)

    # Two flows rather than one union: the checked side carries a watermark from
    # dedupe and the parsed side does not, and separate flows keep that out of question.
    @dp.append_flow(target=QUARANTINE, name=f"{table}_unparseable")
    def unparseable():
        return cdc.unparseable(spark.readStream.table(f"{table}_parsed"), table)

    @dp.append_flow(target=QUARANTINE, name=f"{table}_rejected")
    def rejected():
        return cdc.rejected(spark.readStream.table(f"{table}_checked"), table)


def define_cdc_target(table: str, target: str, scd_type: int, properties: dict) -> None:
    name = f"{CATALOG}.silver.{target}"
    dp.create_streaming_table(name=name, table_properties=properties)
    dp.create_auto_cdc_flow(
        target=name,
        source=f"{table}_valid",
        keys=[cdc.TABLES[table]["pk"]],
        sequence_by=F.col("_lsn"),
        apply_as_deletes=F.expr("_op = 'd'"),
        except_column_list=cdc.META_COLUMNS,
        stored_as_scd_type=scd_type,
    )


dp.create_streaming_table(
    name=QUARANTINE,
    comment="Events kept out of silver: unparseable, non-positive amount, unknown currency.",
)

tombstones = {"pipelines.cdc.tombstoneGCThresholdInSeconds": TOMBSTONE_RETENTION_SECONDS}

for _table in cdc.TABLES:
    define_events(_table)

define_cdc_target("accounts", "accounts", 1, tombstones)
define_cdc_target("accounts", "account_history", 2, {})
define_cdc_target("journal_entries", "journal_entries", 1, tombstones)

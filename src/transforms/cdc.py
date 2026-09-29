"""CDC event parsing, drop rules and the reference silver state.

The pipeline and the tests share everything here. The pipeline hands the cleaned events
to AUTO CDC. The `expected_*` functions compute what AUTO CDC should produce from the
same events, in plain batch Spark, so silver can be checked against them. AUTO CDC does
not run outside Databricks, so this reference is what the unit tests pin down.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Column name -> Spark SQL type in silver. Order matches the data contract.
TABLES = {
    "accounts": {
        "pk": "account_id",
        "columns": {
            "account_id": "STRING",
            "account_number": "STRING",
            "holder_name": "STRING",
            "holder_email": "STRING",
            "business_unit": "STRING",
            "account_type": "STRING",
            "currency": "STRING",
            "status": "STRING",
            "opened_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },
    "journal_entries": {
        "pk": "entry_id",
        "columns": {
            "entry_id": "STRING",
            "journal_id": "STRING",
            "account_id": "STRING",
            "entry_date": "DATE",
            "amount": "DECIMAL(18,2)",
            "side": "STRING",
            "currency": "STRING",
            "description": "STRING",
            "posted_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },
}

OPS = ("c", "u", "d", "r")

# Envelope fields carried alongside the row. AUTO CDC gets these in except_column_list.
META_COLUMNS = ["_op", "_lsn", "_ts_ms", "_tx_id", "_ingested_at", "_source_file"]

CORRUPT_COLUMN = "_corrupt_record"
RESCUED_COLUMN = "_rescued_data"

# Options for reading landing NDJSON. Unparseable lines keep their text in
# _corrupt_record instead of failing the read, so checkpoint 3 can quarantine them.
# rescuedDataColumn is a Databricks option; open source Spark ignores it.
READ_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_COLUMN,
    "rescuedDataColumn": RESCUED_COLUMN,
}


def raw_schema(table: str) -> StructType:
    """Landing schema. Row fields stay strings in bronze; typing happens in silver.

    Keeping bronze untyped means a bad value never fails ingest and the raw text is
    still there to inspect or replay.
    """
    row = StructType([StructField(c, StringType()) for c in TABLES[table]["columns"]])
    return StructType(
        [
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField(
                "source",
                StructType(
                    [
                        StructField("table", StringType()),
                        StructField("lsn", LongType()),
                        StructField("tx_id", LongType()),
                    ]
                ),
            ),
            StructField("before", row),
            StructField("after", row),
            StructField(CORRUPT_COLUMN, StringType()),
        ]
    )


def read_landing(reader, table: str):
    """Apply the landing schema and options to a DataStreamReader or DataFrameReader."""
    return reader.schema(raw_schema(table)).options(**READ_OPTIONS)


def flatten(raw: DataFrame, table: str) -> DataFrame:
    """Envelope -> one typed row per event.

    The row comes from `after`, or from `before` for deletes, where `after` is null.
    Casts use try_cast so a bad value becomes null rather than failing the batch under
    ANSI mode. Null amounts and similar are checkpoint 3 quarantine material.
    """
    cols = [
        F.expr(
            f"try_cast(CASE WHEN op = 'd' THEN before.{name} ELSE after.{name} END"
            f" AS {sql_type})"
        ).alias(name)
        for name, sql_type in TABLES[table]["columns"].items()
    ]
    meta = [
        F.col("op").alias("_op"),
        F.col("source.lsn").alias("_lsn"),
        F.col("ts_ms").alias("_ts_ms"),
        F.col("source.tx_id").alias("_tx_id"),
        _optional(raw, "_ingested_at", "TIMESTAMP"),
        _optional(raw, "_source_file", "STRING"),
    ]
    return raw.select(*cols, *meta)


def _optional(df: DataFrame, name: str, sql_type: str) -> Column:
    """Bronze metadata columns are absent when reading landing files directly."""
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(sql_type).alias(name)


def drop_rules(table: str) -> dict[str, str]:
    """Rows AUTO CDC cannot sequence or key. Used as expect_all_or_drop in the pipeline.

    Written so they never evaluate to NULL: a NULL expectation result is not a clear
    pass or fail, so each rule tests for NULL explicitly.
    """
    pk = TABLES[table]["pk"]
    ops = ", ".join(f"'{o}'" for o in OPS)
    return {
        "pk_present": f"{pk} IS NOT NULL",
        "lsn_present": "_lsn IS NOT NULL",
        "known_op": f"_op IS NOT NULL AND _op IN ({ops})",
    }


def dedupe(events: DataFrame, table: str) -> DataFrame:
    """Collapse exact duplicates on (pk, lsn).

    lsn is unique per change, so two events with the same key and lsn are the same
    change delivered twice. In a stream the state is bounded by a watermark on
    _ingested_at; the generator's duplicates always land in the same file, so they
    arrive in the same micro-batch and an hour of state is plenty.
    """
    keys = [TABLES[table]["pk"], "_lsn"]
    if events.isStreaming:
        return events.withWatermark("_ingested_at", "1 hour").dropDuplicatesWithinWatermark(keys)
    return events.dropDuplicates(keys)


def clean_events(events: DataFrame, table: str) -> DataFrame:
    """Batch version of what the silver source view does: drop rules, then dedupe."""
    condition = " AND ".join(f"({rule})" for rule in drop_rules(table).values())
    return dedupe(events.filter(condition), table)


def expected_scd1(events: DataFrame, table: str) -> DataFrame:
    """Current state: the highest lsn per key wins, and a winning delete removes the row.

    An event older than the current winner changes nothing, which is how late arrivals
    are absorbed. This assumes AUTO CDC keeps delete tombstones longer than the latest
    late arrival; see pipelines.cdc.tombstoneGCThresholdInSeconds in silver.py.
    """
    pk = TABLES[table]["pk"]
    latest = Window.partitionBy(pk).orderBy(F.col("_lsn").desc())
    return (
        events.withColumn("_rank", F.row_number().over(latest))
        .filter("_rank = 1 AND _op != 'd'")
        .select(*TABLES[table]["columns"])
    )


def expected_scd2(events: DataFrame, table: str) -> DataFrame:
    """Full history. Each non-delete event opens a version at its lsn, and the next event
    for the same key (update or delete) closes it. __START_AT and __END_AT carry the
    sequence value, which is what AUTO CDC writes for SCD type 2.
    """
    pk = TABLES[table]["pk"]
    by_lsn = Window.partitionBy(pk).orderBy("_lsn")
    return (
        events.withColumn("__END_AT", F.lead("_lsn").over(by_lsn))
        .filter("_op != 'd'")
        .withColumn("__START_AT", F.col("_lsn"))
        .select(*TABLES[table]["columns"], "__START_AT", "__END_AT")
    )

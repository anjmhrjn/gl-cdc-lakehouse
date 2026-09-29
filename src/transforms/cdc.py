"""CDC event parsing, drop and quarantine rules, and the reference silver state.

The pipeline and the tests share everything here. The pipeline hands the valid events
to AUTO CDC and the rest to silver.quarantine_events. The `expected_*` functions compute
what AUTO CDC should produce from the same events, in plain batch Spark, so silver can
be checked against them. AUTO CDC does not run outside Databricks, so this reference is
what the unit tests pin down.
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

# Currencies the ledger books in. An allowlist rather than all of ISO 4217: "XXX" is a
# real ISO code ("no currency") and still has no place on a ledger line.
CURRENCIES = ("USD", "EUR", "GBP", "JPY")

# Envelope fields carried alongside the row. AUTO CDC gets these in except_column_list.
META_COLUMNS = ["_op", "_lsn", "_ts_ms", "_tx_id", "_ingested_at", "_source_file"]

CORRUPT_COLUMN = "_corrupt_record"
RESCUED_COLUMN = "_rescued_data"

# Carried from flatten to quarantine only; never reaches AUTO CDC.
PAYLOAD_COLUMN = "_payload"
REASONS_COLUMN = "_quarantine_reasons"
UNPARSEABLE = "unparseable_payload"

QUARANTINE_COLUMNS = [
    "source_table",
    "reasons",
    "payload",
    "_op",
    "_lsn",
    "_ingested_at",
    "_source_file",
]

# Options for reading landing NDJSON. Unparseable lines keep their text in
# _corrupt_record instead of failing the read, so silver can quarantine them.
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
    ANSI mode; the quarantine rules then catch the null.

    Also carries _corrupt_record, and _payload: the event as text for quarantine. For an
    unparseable line that is the line itself, otherwise the envelope as JSON.
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
        F.col(CORRUPT_COLUMN),
        F.expr(
            f"CASE WHEN {CORRUPT_COLUMN} IS NOT NULL THEN {CORRUPT_COLUMN}"
            " ELSE to_json(struct(op, ts_ms, source, before, after)) END"
        ).alias(PAYLOAD_COLUMN),
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


def quarantine_rules(table: str) -> dict[str, str]:
    """Rows that can be keyed and sequenced but should not reach silver.

    Kept, not dropped: they go to silver.quarantine_events for someone to look at.
    Each rule is NULL-safe for the same reason as the drop rules. A NULL amount is a
    try_cast failure, which is as unusable as a negative one.
    """
    currencies = ", ".join(f"'{c}'" for c in CURRENCIES)
    rules = {}
    if "amount" in TABLES[table]["columns"]:
        rules["amount_positive"] = "amount IS NOT NULL AND amount > 0"
    rules["currency_known"] = f"currency IS NOT NULL AND currency IN ({currencies})"
    return rules


def quarantine_reasons(table: str) -> Column:
    """Names of the quarantine rules a row fails, in rule order. Empty means valid."""
    checks = ", ".join(
        f"CASE WHEN NOT ({rule}) THEN '{name}' END"
        for name, rule in quarantine_rules(table).items()
    )
    return F.expr(f"array_compact(array({checks}))")


def dedupe(events: DataFrame, table: str) -> DataFrame:
    """Collapse exact duplicates on (pk, lsn).

    lsn is unique per change, so two events with the same key and lsn are the same
    change delivered twice. In a stream the state is bounded by a watermark on
    _ingested_at; the generator's duplicates always land in the same file, so they
    arrive in the same micro-batch and an hour of state is plenty.

    Unparseable rows must not come here: their key and lsn are both null, so every one
    of them would collapse into a single row.
    """
    keys = [TABLES[table]["pk"], "_lsn"]
    if events.isStreaming:
        return events.withWatermark("_ingested_at", "1 hour").dropDuplicatesWithinWatermark(keys)
    return events.dropDuplicates(keys)


def check(events: DataFrame, table: str) -> DataFrame:
    """Parseable events, deduplicated, with the quarantine rules each one fails.

    The pipeline applies the drop rules to this as expect_all_or_drop, so rows that
    cannot be keyed or sequenced are dropped before they are split into valid and
    quarantined.
    """
    parseable = events.filter(f"{CORRUPT_COLUMN} IS NULL")
    return dedupe(parseable, table).withColumn(REASONS_COLUMN, quarantine_reasons(table))


def valid(checked: DataFrame, table: str) -> DataFrame:
    """Checked events that pass every quarantine rule, in the shape AUTO CDC takes."""
    return checked.filter(F.size(REASONS_COLUMN) == 0).select(
        *TABLES[table]["columns"], *META_COLUMNS
    )


def rejected(checked: DataFrame, table: str) -> DataFrame:
    """Checked events that fail a quarantine rule, as quarantine rows."""
    bad = checked.filter(F.size(REASONS_COLUMN) > 0)
    return _quarantine_rows(bad, table, F.col(REASONS_COLUMN))


def unparseable(events: DataFrame, table: str) -> DataFrame:
    """Lines the JSON reader could not parse, as quarantine rows."""
    bad = events.filter(f"{CORRUPT_COLUMN} IS NOT NULL")
    return _quarantine_rows(bad, table, F.array(F.lit(UNPARSEABLE)))


def _quarantine_rows(events: DataFrame, table: str, reasons: Column) -> DataFrame:
    """One schema for both source tables: the row travels as payload text."""
    return events.select(
        F.lit(table).alias("source_table"),
        reasons.alias("reasons"),
        F.col(PAYLOAD_COLUMN).alias("payload"),
        "_op",
        "_lsn",
        "_ingested_at",
        "_source_file",
    )


def _dropped(events: DataFrame, table: str) -> DataFrame:
    """Batch version of the pipeline's expect_all_or_drop."""
    condition = " AND ".join(f"({rule})" for rule in drop_rules(table).values())
    return events.filter(condition)


def clean_events(events: DataFrame, table: str) -> DataFrame:
    """Batch version of what reaches AUTO CDC: parseable, not dropped, deduplicated,
    and passing every quarantine rule."""
    return valid(check(_dropped(events, table), table), table)


def quarantined(events: DataFrame, table: str) -> DataFrame:
    """Batch version of what the pipeline appends to silver.quarantine_events.

    Unparseable lines are taken before the drop rules. They have no key, so the
    missing-PK rule would otherwise drop them without a trace.
    """
    checked = check(_dropped(events, table), table)
    return unparseable(events, table).unionByName(rejected(checked, table))


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

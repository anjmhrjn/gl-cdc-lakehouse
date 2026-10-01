"""Bronze: landing NDJSON -> append-only Delta, one table per source table.

Nothing is dropped or typed here. Row fields stay strings, unparseable lines keep their
text in _corrupt_record, and unexpected fields go to _rescued_data. Silver decides what
is usable, and bronze stays a faithful copy that silver can be rebuilt from.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transforms import cdc

CATALOG = spark.conf.get("gl.catalog")
LANDING_ROOT = spark.conf.get("gl.landing_root")
# Files per micro-batch. 1000 is Auto Loader's own default. The compaction experiment
# in docs/results.md sets 30, so one full refresh writes the way a day of 30 minute
# scheduled updates would: about 30 new landing files per table per batch.
MAX_FILES_PER_TRIGGER = spark.conf.get("gl.max_files_per_trigger")


def define_bronze(table: str) -> None:
    @dp.table(
        name=f"{CATALOG}.bronze.{table}_cdc_raw",
        comment=f"Raw CDC events for {table} as landed, plus ingest metadata. Append-only.",
    )
    def raw():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        )
        return (
            cdc.read_landing(reader, table)
            .load(f"{LANDING_ROOT}/{table}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for _table in cdc.TABLES:
    define_bronze(_table)

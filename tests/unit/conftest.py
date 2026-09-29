import json

import pytest
from pyspark.sql import SparkSession

from transforms import cdc


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("gl-cdc-unit")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def event(op, lsn, table="accounts", before=None, after=None):
    return {
        "op": op,
        "ts_ms": 0,
        "source": {"table": table, "lsn": lsn, "tx_id": 1},
        "before": before,
        "after": after,
    }


def account(account_id, status="OPEN", name="Alice Adeyemi", updated_at="2026-09-01T00:00:00.000Z"):
    return {
        "account_id": account_id,
        "account_number": "123456789012",
        "holder_name": name,
        "holder_email": "alice.adeyemi@example.com",
        "business_unit": "TREASURY",
        "account_type": "ASSET",
        "currency": "USD",
        "status": status,
        "opened_at": "2026-01-01T00:00:00.000Z",
        "updated_at": updated_at,
    }


def entry(entry_id, amount="100.00", side="DEBIT", description="Fee accrual"):
    return {
        "entry_id": entry_id,
        "journal_id": "JRN-00000001",
        "account_id": "ACC-000001",
        "entry_date": "2026-09-01",
        "amount": amount,
        "side": side,
        "currency": "USD",
        "description": description,
        "posted_at": "2026-09-01T00:00:00.000Z",
        "updated_at": "2026-09-01T00:00:00.000Z",
    }


@pytest.fixture
def load_flat(spark, tmp_path):
    """Write records as an NDJSON landing file and read them back the way bronze does.

    Records can be dicts or raw strings, so malformed lines go through the real parser.
    Returns every flattened event, before drop rules, dedupe and quarantine.
    """
    counter = iter(range(10**6))

    def _load(table, records):
        path = tmp_path / f"{table}-{next(counter)}.json"
        lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
        path.write_text("\n".join(lines) + "\n")
        # Cached because Spark refuses a query on raw JSON that reads only _corrupt_record,
        # which is all a count() of unparseable rows needs.
        raw = cdc.read_landing(spark.read, table).json(str(path)).cache()
        return cdc.flatten(raw, table)

    return _load


@pytest.fixture
def load_events(load_flat):
    """Like load_flat, but only the events that reach AUTO CDC."""

    def _load(table, records):
        return cdc.clean_events(load_flat(table, records), table)

    return _load


def generated_events(spark, root):
    """Flattened events per table from a local generator run under `root`.

    A table the run wrote nothing for is left out.
    """
    events = {}
    for table in cdc.TABLES:
        path = root / "dev" / "cdc" / table
        if not path.exists():
            continue
        path = str(path)
        # Cached because Spark refuses a query on raw JSON that reads only _corrupt_record.
        raw = cdc.read_landing(spark.read, table).json(path).cache()
        events[table] = cdc.flatten(raw, table)
    return events

"""Mask and row filter functions from sql/governance/01_functions.sql.

The tests run the real SQL, not a Python copy of it. Each function is created as a
temporary function, next to a stub `is_account_group_member` that answers from a fixed
list of groups, so every case says exactly whose view it checks.
"""

from pathlib import Path

import pytest
from pyspark.sql.types import StructType

from jobs.governance import statements
from transforms import cdc

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "governance"


@pytest.fixture
def as_member(spark):
    """Recreate the governance functions for a caller who is in exactly `groups`."""
    functions = [
        s.replace("CREATE OR REPLACE FUNCTION silver.", "CREATE OR REPLACE TEMPORARY FUNCTION ")
        for s in statements((SQL_DIR / "01_functions.sql").read_text())
    ]

    def _as(*groups):
        members = ", ".join(f"'{g}'" for g in groups) or "NULL"
        spark.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION is_account_group_member(g STRING)"
            f" RETURNS BOOLEAN RETURN coalesce(g IN ({members}), false)"
        )
        for s in functions:
            spark.sql(s)

    return _as


def one(spark, expr):
    return spark.sql(f"SELECT {expr} AS v").first()["v"]


def test_account_number_shows_last_four(spark, as_member):
    as_member("gl_engineers")
    assert one(spark, "mask_account_number('123456789012')") == "********9012"


def test_account_number_short_value_is_fully_masked(spark, as_member):
    as_member()
    assert one(spark, "mask_account_number('123')") == "***"
    assert one(spark, "mask_account_number('1234')") == "****"


def test_account_number_null_stays_null(spark, as_member):
    as_member()
    assert one(spark, "mask_account_number(CAST(NULL AS STRING))") is None


def test_pii_reader_sees_account_number(spark, as_member):
    as_member("gl_pii_readers")
    assert one(spark, "mask_account_number('123456789012')") == "123456789012"


def test_pii_is_redacted(spark, as_member):
    as_member("gl_engineers")
    assert one(spark, "mask_pii('Alice Adeyemi')") == "REDACTED"
    assert one(spark, "mask_pii(CAST(NULL AS STRING))") is None


def test_pii_reader_sees_pii(spark, as_member):
    as_member("gl_pii_readers")
    assert one(spark, "mask_pii('alice.adeyemi@example.com')") == "alice.adeyemi@example.com"


def test_payload_redacted_only_for_accounts(spark, as_member):
    as_member("gl_engineers")
    assert one(spark, "mask_payload('{\"holder_name\": \"Alice\"}', 'accounts')") == "REDACTED"
    assert one(spark, "mask_payload('{\"amount\": \"-5\"}', 'journal_entries')") == (
        '{"amount": "-5"}'
    )


def test_pii_reader_sees_account_payload(spark, as_member):
    as_member("gl_pii_readers")
    assert one(spark, "mask_payload('{}', 'accounts')") == "{}"


ROW = (
    "named_struct('account_id', 'ACC-000001', 'account_number', '123456789012',"
    " 'holder_name', 'Alice Adeyemi', 'holder_email', 'alice.adeyemi@example.com',"
    " 'business_unit', 'TREASURY', 'account_type', 'ASSET', 'currency', 'USD',"
    " 'status', 'OPEN', 'opened_at', '2026-01-01T00:00:00.000Z',"
    " 'updated_at', '2026-09-01T00:00:00.000Z')"
)
ROW_TYPE = cdc.raw_schema("accounts")["after"].dataType.simpleString()


def test_account_row_masks_pii_and_keeps_the_rest(spark, as_member):
    as_member("gl_engineers")
    row = one(spark, f"mask_account_row({ROW})").asDict()
    assert row == {
        "account_id": "ACC-000001",
        "account_number": "********9012",
        "holder_name": "REDACTED",
        "holder_email": "REDACTED",
        "business_unit": "TREASURY",
        "account_type": "ASSET",
        "currency": "USD",
        "status": "OPEN",
        "opened_at": "2026-01-01T00:00:00.000Z",
        "updated_at": "2026-09-01T00:00:00.000Z",
    }


def test_account_row_type_matches_bronze(spark, as_member):
    """A mask on a STRUCT column must return the column's exact struct type."""
    as_member()
    got = spark.sql(f"SELECT mask_account_row({ROW}) AS v").schema["v"].dataType
    bronze = cdc.raw_schema("accounts")["after"].dataType
    assert isinstance(got, StructType)
    assert got.simpleString() == bronze.simpleString()


def test_account_row_null_stays_null(spark, as_member):
    """`before` is null for creates and `after` is null for deletes."""
    as_member()
    assert one(spark, f"mask_account_row(CAST(NULL AS {ROW_TYPE}))") is None


def test_pii_reader_sees_account_row(spark, as_member):
    as_member("gl_pii_readers")
    assert one(spark, f"mask_account_row({ROW})") == one(spark, ROW)


def visible_units(spark):
    units = ["TREASURY", "CORP_BANKING", "ASSET_MGMT", None]
    values = ", ".join("(CAST(NULL AS STRING))" if u is None else f"('{u}')" for u in units)
    rows = spark.sql(
        f"SELECT business_unit FROM VALUES {values} AS t(business_unit)"
        " WHERE bu_filter(business_unit)"
    ).collect()
    return {r["business_unit"] for r in rows}


def test_engineers_see_every_unit(spark, as_member):
    as_member("gl_engineers")
    assert visible_units(spark) == {"TREASURY", "CORP_BANKING", "ASSET_MGMT", None}


@pytest.mark.parametrize(
    "group, unit",
    [
        ("gl_analysts_treasury", "TREASURY"),
        ("gl_analysts_corp_banking", "CORP_BANKING"),
        ("gl_analysts_asset_mgmt", "ASSET_MGMT"),
    ],
)
def test_analysts_see_only_their_unit(spark, as_member, group, unit):
    as_member(group)
    assert visible_units(spark) == {unit}


def test_analyst_in_two_groups_sees_both_units(spark, as_member):
    as_member("gl_analysts_treasury", "gl_analysts_asset_mgmt")
    assert visible_units(spark) == {"TREASURY", "ASSET_MGMT"}


def test_no_group_sees_nothing(spark, as_member):
    as_member("gl_pii_readers")
    assert visible_units(spark) == set()


def test_statements_ignore_semicolons_in_comments():
    text = "-- first; not a statement\nSELECT 1;\n\n  -- note;\nSELECT 2;\n"
    assert statements(text) == ["SELECT 1", "SELECT 2"]


def test_governance_sql_is_rerunnable():
    """Every statement replaces, alters or grants, so the job can run any number of times."""
    for path in sorted(SQL_DIR.glob("*.sql")):
        for stmt in statements(path.read_text()):
            rerunnable = stmt.startswith(("CREATE OR REPLACE ", "ALTER ", "GRANT "))
            assert rerunnable, f"{path.name}: {stmt}"

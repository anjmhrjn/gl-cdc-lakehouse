"""Row and event shapes for the two source tables.

The rows mirror the data contract in CLAUDE.md. Amounts are held as integer cents
internally and only formatted as a 2dp decimal string on the way out, so the
DECIMAL(18,2) contract survives JSON round-tripping without float drift.
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

BUSINESS_UNITS = ("TREASURY", "CORP_BANKING", "ASSET_MGMT")
ACCOUNT_TYPES = ("ASSET", "LIABILITY", "EQUITY", "REVENUE", "EXPENSE")
CURRENCIES = ("USD", "EUR", "GBP", "JPY")
STATUSES = ("OPEN", "FROZEN", "CLOSED")

_FIRST_NAMES = (
    "Alice", "Bruno", "Chen", "Dara", "Elif", "Farid", "Gita", "Hana",
    "Ivan", "Jonas", "Kiri", "Lena", "Malik", "Nadia", "Omar", "Priya",
)
_LAST_NAMES = (
    "Adeyemi", "Barros", "Chaudhary", "Dumont", "Eriksson", "Fontaine",
    "Ghale", "Halvorsen", "Iqbal", "Jansen", "Kovacs", "Lindqvist",
)
DESCRIPTIONS = (
    "FX settlement", "Interbank transfer", "Fee accrual", "Coupon payment",
    "Payroll allocation", "Intercompany netting", "Custody charge", "Rebalance",
)


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def money(cents: int) -> str:
    """Format integer cents as a fixed 2dp string, e.g. 123456 -> '1234.56'."""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def make_account(rng: random.Random, account_id: str, now: datetime) -> dict:
    opened = now - timedelta(days=rng.randint(30, 3000), minutes=rng.randint(0, 1440))
    first = rng.choice(_FIRST_NAMES)
    last = rng.choice(_LAST_NAMES)
    return {
        "account_id": account_id,
        "account_number": f"{rng.randint(10**11, 10**12 - 1)}",
        "holder_name": f"{first} {last}",
        "holder_email": f"{first.lower()}.{last.lower()}@example.com",
        "business_unit": rng.choice(BUSINESS_UNITS),
        "account_type": rng.choice(ACCOUNT_TYPES),
        "currency": rng.choice(CURRENCIES),
        "status": "OPEN",
        "opened_at": iso(opened),
        "updated_at": iso(opened),
    }


def mutate_account(rng: random.Random, row: dict, now: datetime) -> dict:
    """Return a copy of an account row with one realistic field change applied."""
    new = dict(row)
    field = rng.choice(["status", "holder_email", "holder_name", "account_type"])
    if field == "status":
        new["status"] = rng.choice([s for s in STATUSES if s != row["status"]])
    elif field == "holder_email":
        local = row["holder_name"].lower().replace(" ", ".")
        new["holder_email"] = f"{local}@{rng.choice(('example.com', 'example.org'))}"
    elif field == "holder_name":
        new["holder_name"] = f"{row['holder_name'].split()[0]} {rng.choice(_LAST_NAMES)}"
    else:
        new["account_type"] = rng.choice(ACCOUNT_TYPES)
    new["updated_at"] = iso(now)
    return new


def _split_cents(rng: random.Random, total: int, parts: int) -> list[int]:
    """Split total into `parts` positive integers. Distinct cuts keep every part > 0."""
    if parts == 1:
        return [total]
    cuts = sorted(rng.sample(range(1, total), parts - 1))
    out = []
    prev = 0
    for cut in cuts:
        out.append(cut - prev)
        prev = cut
    out.append(total - prev)
    return out


def make_journal(
    rng: random.Random,
    journal_id: str,
    accounts: list[dict],
    now: datetime,
    entry_day: date,
    entry_prefix: str,
    start_index: int,
) -> list[dict]:
    """Build one balanced journal.

    Every line is drawn from accounts that share a business_unit and a currency.
    That is stricter than the per-journal_id invariant in the data contract, but it
    is what makes trial_balance_check pass: that job nets debits against credits
    per business_unit, currency and entry_date, so a journal spanning two units
    would leave both sides unbalanced.
    """
    debit_lines = rng.randint(1, 3)
    credit_lines = rng.randint(1, 3)
    total_cents = rng.randint(100_00, 5_000_000_00)
    amounts = [
        (acct, "DEBIT", cents)
        for acct, cents in zip(
            rng.choices(accounts, k=debit_lines),
            _split_cents(rng, total_cents, debit_lines),
            strict=True,
        )
    ] + [
        (acct, "CREDIT", cents)
        for acct, cents in zip(
            rng.choices(accounts, k=credit_lines),
            _split_cents(rng, total_cents, credit_lines),
            strict=True,
        )
    ]

    posted = now - timedelta(seconds=rng.randint(0, 600))
    rows = []
    for offset, (acct, side, cents) in enumerate(amounts):
        rows.append(
            {
                "entry_id": f"{entry_prefix}-{start_index + offset:06d}",
                "journal_id": journal_id,
                "account_id": acct["account_id"],
                "entry_date": entry_day.isoformat(),
                "amount": money(cents),
                "side": side,
                "currency": acct["currency"],
                "description": rng.choice(DESCRIPTIONS),
                "posted_at": iso(posted),
                "updated_at": iso(posted),
            }
        )
    return rows


def envelope(op: str, table: str, lsn: int, tx_id: int, at: datetime, before, after) -> dict:
    """Debezium-shaped change event. `lsn` is the only ordering key downstream."""
    return {
        "op": op,
        "ts_ms": ms(at),
        "source": {"table": table, "lsn": lsn, "tx_id": tx_id},
        "before": before,
        "after": after,
    }

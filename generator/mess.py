"""Deliberate defects injected into the event stream.

Everything here is driven by the run's seeded Random, so a run with the same
seed produces byte-identical output. That is what makes the idempotent-rerun
tests in tests/integration meaningful.
"""

from __future__ import annotations

import copy
import json
import random

# Rates come from the data contract in CLAUDE.md.
DUPLICATE_RATE = 0.02
MALFORMED_RATE = 0.005
LATE_RATE = 0.015


def duplicate(rng: random.Random, events: list, rate: float = DUPLICATE_RATE) -> list:
    """Append exact copies of some events. Silver must collapse these by (pk, lsn)."""
    out = list(events)
    for event in events:
        if rng.random() < rate:
            out.append(copy.deepcopy(event))
    return out


def shuffle_within_file(rng: random.Random, events: list) -> list:
    """Events land out of order inside a single file, so arrival order is not lsn order."""
    out = list(events)
    rng.shuffle(out)
    return out


def malformed_record(rng: random.Random, table: str, lsn: int) -> str | dict:
    """One bad record.

    Half of these are unparseable text, which bronze rescues and silver quarantines.
    The rest are structurally valid JSON that must be dropped (missing PK, null lsn,
    unknown op) or quarantined (amount <= 0, unknown currency).
    """
    kind = rng.choice(
        ["truncated", "not_json", "missing_pk", "null_lsn", "unknown_op", "bad_amount", "bad_ccy"]
    )
    base = {
        "op": "u",
        "ts_ms": 0,
        "source": {"table": table, "lsn": lsn, "tx_id": 0},
        "before": None,
        "after": {},
    }
    pk = "account_id" if table == "accounts" else "entry_id"

    if kind == "truncated":
        return json.dumps(base)[: rng.randint(10, 40)]
    if kind == "not_json":
        return f"<<corrupt record lsn={lsn}>>"
    if kind == "missing_pk":
        if table == "accounts":
            base["after"] = {"business_unit": "TREASURY"}
        else:
            base["after"] = {"amount": "10.00"}
        return base
    if kind == "null_lsn":
        base["source"]["lsn"] = None
        base["after"] = {pk: f"BAD-{lsn}"}
        return base
    if kind == "unknown_op":
        base["op"] = rng.choice(["x", "t", ""])
        base["after"] = {pk: f"BAD-{lsn}"}
        return base
    if kind == "bad_amount":
        base["after"] = {pk: f"BAD-{lsn}", "amount": rng.choice(["0.00", "-42.50"])}
        return base
    base["after"] = {pk: f"BAD-{lsn}", "currency": rng.choice(["XXX", "ZZ", "us_dollar"])}
    return base

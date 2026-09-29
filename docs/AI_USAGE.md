# AI usage

Claude wrote most of the code in this repo. This file logs what it got wrong, how that
was caught, and what the fix was. The point is an honest record, not a highlight reel.

## Hot account share came out at 34 percent instead of 30

**Produced:** the generator picked the hot account's (business unit, currency) group
with probability 0.5 and, inside those journals, put each line on the hot account with
probability 0.6, aiming at the 30 percent share the data contract asks for.

**Wrong:** the fallback branch chose a group at random from all groups, including the
hot group. So hot group journals were selected more often than 0.5 and the share came
out at 34 percent.

**Caught by:** a verification script over the generated files that counted lines per
account. It reported `hot account ACC-000017: 575/1693 journal lines = 34.0%`.

**Fix:** the fallback now chooses from groups other than the hot group. Measured share
is 32.6 percent on a 2000 event run, which is within the variance of a run that also
deletes whole journals.

## Catalogs were created with no storage root

**Produced:** `databricks_catalog` resources with no `storage_root`, on the reasoning
that managed tables should live in the metastore's default managed storage and stay
separate from the landing bucket. That reasoning was written into ARCHITECTURE.md as a
deliberate decision.

**Wrong:** the assumption that the metastore had a storage root to fall back on. It does
not. The account has Default Storage enabled, and the metastore summary has no
`storage_root` field, which I read as a detail rather than the constraint it was. A
catalog created that way is rejected outright:

```
cannot create catalog: Metastore storage root URL does not exist. Default Storage is
enabled in your account.
```

**Caught by:** `terraform apply`, run by Anuj. `terraform plan` cannot catch it, because
the failure only happens when the Unity Catalog API is actually called. Everything
before the catalog applied cleanly, so the run got as far as the S3 bucket, the KMS key,
the IAM role, the storage credential and the landing external location before stopping.

**Fix:** a second external location per environment at `<env>/managed`, granted
`CREATE_MANAGED_STORAGE`, with each catalog's `storage_root` pointing at it. The
ARCHITECTURE.md section that argued for the old design has been rewritten rather than
quietly deleted. Re-planning showed 10 to add, 0 to change, 0 to destroy, so nothing
that had already been created was disturbed.

**Worth noting:** the error also shows the limit of `terraform plan` as a check. Provider
side validation of a resource that does not exist yet is not a thing plan can do.

## Generator restarted lsn at the same value every run

**Produced:** the checkpoint 1 generator started every run's `lsn` counter at 5,000,000
and drew late arrivals from a fixed band below it.

**Wrong:** `lsn` must be monotonic across runs, not only within one. Two runs with the
same seed produce the same keys and the same lsns, but with different timestamps, so
the landing prefix held different rows under the same (key, lsn). AUTO CDC cannot order
those, and the result depends on which row it happens to see. The dev landing prefix
already had two such runs from 2026-09-28.

**Caught by:** me, while designing the checkpoint 2 integration check. Comparing silver
to a reference only works if the input has one right answer, and this input did not. A
script over two local runs confirmed the overlap.

**Fix:** `lsn` is now the run's time in milliseconds times 100 plus a counter, and a
late event gets the lsn of its backdated time. Two back-to-back local runs now produce
zero conflicting (key, lsn) pairs. Anuj removed the old dev landing files before the
first pipeline run, and silver then matched the reference exactly.

## Reconciliation test read stale cached data

**Produced:** a unit test that runs the generator with `--withhold-late-accounts`,
computes gold, runs it again with `--only-late-accounts` into the same directory, and
expects every orphan to be gone.

**Wrong:** the test helper caches the raw JSON read, to get around Spark refusing a
query that reads only `_corrupt_record`. The second read of the same path matched the
cached plan from the first read, so it never saw the new files. The test reported five
orphan groups after the late accounts had landed.

**Caught by:** the test itself failing, then a fresh process over the same two runs
showing zero orphans. So the generator was right and the test was reading old data.

**Fix:** the test calls `spark.catalog.clearCache()` between the two runs, with a comment
saying why.

## Generator ids restarted at 1 every run

**Produced:** the checkpoint 1 generator numbered journals and journal lines with
counters that started at 1 in every run (`JRN-00000001`, `JE-000000001`). New accounts
were numbered on from the base accounts, so they restarted too. Landing file names used
the minute and a per-run counter.

**Wrong:** a CDC source never reuses a primary key for a different row. Each run
overwrote the previous run's journal lines by key instead of adding to them, so row
counts stopped growing across runs. That would have distorted the tuning experiments
and the replay tests. Two runs started in the same minute also wrote the same S3 keys.
The pipeline and the reference both handled this correctly, which is why no check
failed.

**Caught by:** Anuj's checkpoint 3 dev run. `silver.journal_entries` had 415 rows after
two full generator runs, against 410 after one in checkpoint 2.

**Fix:** a run tag (the run's start time in milliseconds) in journal ids, entry ids,
ids of accounts opened during the run, and file names. `lsn` is anchored to the same
unrounded time instead of the minute. The base accounts keep stable ids on purpose. A
unit test runs the generator twice in the same minute with the same seed and checks
that the second run adds files, journals and lines instead of replacing them.

## Told Anuj to expect orphans that could not appear

**Produced:** dev run instructions saying the first `silver_state_check` after the
`--withhold-late-accounts` run would show orphan groups in the trial balance.

**Wrong:** I did not check what the landing prefix already held. The held-back accounts
are base accounts with fixed ids, and the checkpoint 2 run had already landed them. So
the journal lines found their accounts and there were no orphans. The unit test for
reconciliation passed because it starts from an empty directory.

**Caught by:** Anuj's run: 0 orphan groups in the first check.

**Fix:** the reconciliation proof runs on an empty dev landing prefix. ARCHITECTURE.md
now states that the late accounts are only late on an empty prefix.

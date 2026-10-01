# Results

Tuning experiments and cost, measured on dev. Each experiment says what changed, the
before and after numbers, and why the result came out the way it did. The numbers come
from the `tuning_probe` and `maintenance` jobs (`src/jobs/`) and from the system table
queries in `sql/analysis/`, so every figure here can be produced again.

## Dataset

Generated 2026-09-30 by Anuj:

```
uv run python -m generator --env dev --minutes 1440 --rate 1000
```

One simulated day, one file per table per minute: 2,880 landing files on top of the
small amount left from checkpoints 1 to 5.

| Table | Rows |
|---|---|
| bronze.journal_entries_cdc_raw | 944,516 events |
| bronze.accounts_cdc_raw | 152,758 events |
| silver.journal_entries | 1,135,575 |
| silver.account_history | 214,556 |
| silver.accounts | 10,811 |
| silver.quarantine_events | 3,226 |

The hot account, ACC-000017, holds 351,925 of 1,135,575 journal lines (31%).

## Correctness across every run

Every configuration below ended with `silver_state_check` passing, with 0 missing and
0 extra rows in every table and the same checksums each time:

| Table | Checksum |
|---|---|
| silver.accounts | -149004982767139277849 |
| silver.account_history | -962282899742779445001 |
| silver.journal_entries | 879878267475906258367 |
| silver.quarantine_events | -552176939629928834311 |
| gold.account_balances | -123632803030168765432 |
| gold.daily_trial_balance | -995449021005676728 |

The same input gave identical tables whether bronze read it in one batch or in 48, with
or without clustering, and with any file size. That is the idempotency requirement
holding on a million-row dataset, not only on the checkpoint 5 sample.

The first baseline run failed this check. The pipeline was right and the SCD2
reference was wrong; see `docs/AI_USAGE.md`.

## Pipeline update duration

Full refreshes over the whole dataset, from the event log (`tuning_probe mode=stats`):

| Run | Setting | Update seconds |
|---|---|---|
| A | defaults, one batch | 172.5 |
| B | 30 files per batch (48 batches) | 177.5 |
| D | 1 MB target files, no clustering | 168.5 |
| E | 1 MB target files, clustered | 103.7 |
| final | defaults, clustered | 107.0 |

Serverless start-up and scheduling are part of each figure and vary from run to run, so
differences under about a minute are not evidence of anything. E and the final run were
the last two and ran back to back.

## Experiment 1: small-file compaction

**Question:** do the scheduled updates leave many small files, and does OPTIMIZE help?

**Setup:** a full refresh reads every landing file in one batch, which writes a handful
of files and hides the problem. A 30 minute schedule instead writes about 30 new files
per table per update. Run B reproduces that in one update by setting Auto Loader's
`cloudFiles.maxFilesPerTrigger` to 30 (bundle variable `max_files_per_trigger`, default
1000, which is Auto Loader's own default). The day's 1,440 files per table then go
through in 48 micro-batches. Then `maintenance` ran OPTIMIZE.

```
databricks bundle deploy -t dev --var max_files_per_trigger=30
databricks bundle run gl_pipeline -t dev --var max_files_per_trigger=30 --full-refresh-all
databricks bundle run maintenance -t dev --var max_files_per_trigger=30
```

| Table | A: one batch | B: 48 batches | B after OPTIMIZE |
|---|---|---|---|
| bronze.journal_entries_cdc_raw | 2 files, 30.4 MB | 23 files, 28.7 MB | 1 file, 29.2 MB |
| bronze.accounts_cdc_raw | 2 files, 6.2 MB | 13 files, 7.5 MB | 1 file, 6.0 MB |
| silver.journal_entries | 1 file, 17.5 MB | 1 file, 19.2 MB | 1 file, 19.2 MB |
| silver.account_history | 1 file, 3.4 MB | 1 file, 3.4 MB | 1 file, 3.4 MB |
| silver.accounts | 1 file, 0.26 MB | 1 file, 0.26 MB | 1 file, 0.26 MB |
| silver.quarantine_events | 4 files, 75 KB | 4 files, 78 KB | 1 file, 68 KB |

**Result:** the small-file problem mostly does not happen here.

- Bronze got 23 files from 48 appends, not 48 or more. Auto compaction merged some of
  them during the update.
- Silver stayed at one file per table. AUTO CDC writes with MERGE, and Databricks
  always runs optimized writes and auto compaction for MERGE.
- OPTIMIZE brought bronze to one file per table. Silver had nothing to compact.
- The update took about as long either way (177.5 against 172.5 seconds).

**Why:** Unity Catalog managed tables autotune file size, and MERGE always compacts. On
top of that, predictive optimization is enabled on this metastore and runs OPTIMIZE and
VACUUM on these tables in the background. So `maintenance` has no schedule. It is kept
for after a large backfill, and for experiments that need compaction at a known moment.

## Experiment 2: liquid clustering on journal_entries

**Question:** does clustering `silver.journal_entries` on `(account_id, entry_date)` let
reads by account or by day skip files?

**Setup:** at 17.5 MB, `journal_entries` is a single file, since autotuning targets
256 MB for small tables. With one file there is nothing to skip, and the result would
be "no difference" at any clustering. So for this experiment only,
`delta.targetFileSize` was set to `1mb` (bundle variable `journal_target_file_size`,
default `auto`). That imitates the file layout of a table a few hundred times larger.
It is a simulation, and the numbers are about file skipping, not about real query
speed at this size.

Run D had no clustering; run E had `cluster_by=["account_id", "entry_date"]`. Each got a
full refresh, then OPTIMIZE on `silver.journal_entries`, then the benchmark queries
(`tuning_probe mode=queries`):

```
databricks bundle deploy -t dev --var journal_target_file_size=1mb
databricks bundle run gl_pipeline -t dev --var journal_target_file_size=1mb --full-refresh-all
databricks bundle run maintenance -t dev --var journal_target_file_size=1mb --params tables=silver.journal_entries,vacuum=false
databricks bundle run tuning_probe -t dev --var journal_target_file_size=1mb --params mode=queries,label=D_unclustered
```

| | D: not clustered | E: clustered |
|---|---|---|
| files after OPTIMIZE | 3 | 25 |
| bytes | 18.1 MB | 29.3 MB |

The 1 MB target did not split D's files. The pipeline's MERGE wrote about 6 MB files,
and OPTIMIZE only merges small files, it never splits large ones. Clustering rewrote E
at the target size.

Benchmark queries (the same in every run; `hot` is ACC-000017, `cold` is ACC-000175, the
day is 2026-09-30):

| Query | Filter | Rows |
|---|---|---|
| cold_account_day | account_id = cold AND entry_date = day | 881 |
| cold_account | account_id = cold | 3,147 |
| hot_account_day | account_id = hot AND entry_date = day | 102,407 |
| one_day | entry_date = day | 328,849 |

Files and bytes read, from `system.query.history` (`sql/analysis/benchmark_history.sql`,
run by hand; at the time it was a `tuning_probe` mode, removed since, see Cost). Bytes
are what the query read after column pruning, so they are well under the table size.

| Query | D: files read | D: bytes | E: files read | E: files skipped | E: bytes | Bytes, E / D |
|---|---|---|---|---|---|---|
| cold_account_day | 3 | 7.03 MB | 1 | 24 | 0.56 MB | 8% |
| cold_account | 3 | 6.78 MB | 4 | 21 | 1.94 MB | 29% |
| hot_account_day | 3 | 7.03 MB | 2 | 23 | 0.95 MB | 14% |
| one_day | 3 | 5.85 MB | 7 | 18 | 3.14 MB | 54% |

In D no file was skipped: every file held some of every account and day. Query
durations were 0.5 to 0.75 seconds in both runs, which at this size is mostly fixed
overhead, so they say nothing about clustering.

**Result:** with the table spread over 25 files, clustering let a query for one
account on one day read 1 file and skip 24, and read 8% of the bytes. The more
selective the filter, the bigger the saving. A filter on the day alone saved least,
because `entry_date` is the second clustering key and a day spans several accounts'
files. Runs A to C, for comparison, read the whole single file every time (1 file,
0 skipped).

**Why:** liquid clustering sorts rows by the clustering keys when it writes files, so
each file covers a narrow range of `account_id` and `entry_date`. Delta keeps min and
max values per file, and a query skips every file whose range cannot match. Without
clustering, rows land in files in whatever order the MERGE wrote them, so every file's
range covers nearly everything.

**Decision:** `cluster_by` stays on `journal_entries`, with the file size back on
`auto`. At the current size it makes no difference either way, and the only cost is
some clustering work inside the OPTIMIZE runs predictive optimization already does. It is the layout the
table needs once it grows past one file.

## Experiment 3: the skewed join on the hot account

**Question:** does the hot account (31% of journal lines) slow the gold join of journal
lines to accounts?

**Setup:** `tuning_probe mode=skew` runs the `daily_trial_balance` join and aggregate
three times with each join strategy: a forced shuffle join (sort-merge, which sends
every line of one account to one task), Spark's own choice, and a forced broadcast of
the accounts side.

```
databricks bundle run tuning_probe -t dev --params mode=skew
```

| Strategy | Seconds (3 runs) | Median |
|---|---|---|
| forced shuffle | 12.96, 1.85, 1.68 | 1.85 |
| Spark's choice | 1.70, 1.30, 1.44 | 1.44 |
| forced broadcast | 2.38, 1.65, 1.52 | 1.65 |

The first run of the whole job includes warm-up, which is why it is 13 seconds.

**Result:** no skew problem to fix at this scale, so the code was not changed.

**Why (explanation, not measured):**

- The accounts side is small: one row per account, about 11,000 rows. Spark's adaptive
  execution can switch a join to a broadcast at run time when one side turns out to be
  this small, and with a broadcast join the hot account's lines never gather in one
  task. Spark's own choice matched the forced broadcast within noise, which fits that,
  but the final plan was not captured, so this is the likely reason rather than a
  checked one.
- Even the forced shuffle was only 0.4 seconds slower. Adaptive execution is built to
  split a skewed partition of a sort-merge join into smaller tasks, and the aggregation
  after the join sums rows on each task before the shuffle.
- An explicit broadcast hint would not make it faster, and would stay in force if the
  accounts side ever grew too large to broadcast well.

Skew would matter if both sides were large, for example a join of journal lines to
another table of lines keyed on account. Then salting the hot key would be the fix.

## Cost

The cost query is `sql/analysis/cost_per_update.sql`: DBUs per pipeline update
(`usage_metadata.dlt_update_id`) from `system.billing.usage`, priced with
`system.billing.list_prices` at `pricing.effective_list.default`. These are list prices,
not what the account is invoiced.

It is run by a person in the SQL editor or a notebook, not by a job. Jobs run as
`gl-cicd`, and the system tables (billing, query history) are kept readable by people
only: billing covers the whole account, and the CI identity has no need for it. During
the experiments the query was a `tuning_probe` mode, which worked while jobs ran as
Anuj and failed with `INSUFFICIENT_PERMISSIONS` once they ran as `gl-cicd`.

Billing records arrive hours after the usage. The experiment runs (21:03 to 22:01 UTC
on 2026-09-30) were not in billing on the evening they ran; their figures below were
read once they had arrived.

**Checkpoint 2 to 5 pipeline updates** (about 500 journal events per run), 17 updates on
2026-09-29 and 2026-09-30:

| | DBUs | List USD |
|---|---|---|
| cheapest update | 0.074 | 0.026 |
| most expensive update | 0.434 | 0.152 |
| typical update | about 0.3 | about 0.10 |

The list price came to 0.35 USD per DBU for serverless pipelines on these rows. One
group of pipeline usage on 2026-09-29 has no update id: 4.898 DBUs, 1.714 USD. The
billing docs list `dlt_maintenance_id` for pipeline maintenance tasks, which this query
does not group by, so that is the likely source but it has not been checked.

Job runs on serverless (state checks, governance, replay) cost 0.003 to 0.184 USD each.

**Checkpoint 6 experiment updates**, full refreshes over the medium dataset (1.1M
events), all on SKU `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO`:

| Run | Update id | DBUs | List USD |
|---|---|---|---|
| A: defaults, one batch | 5dbe95ba-b549-4f00-b882-751c21f34196 | 0.511 | 0.179 |
| B: 30 files per batch | cb5f41dc-31e5-4972-9639-e58897fac346 | 0.529 | 0.185 |
| D: 1 MB files, not clustered | 4f4b1b91-c979-495d-a4a0-f0bc0a85e034 | 0.516 | 0.181 |
| E: 1 MB files, clustered | 6f0985b5-b14a-458c-8f8f-89c88b9e0826 | 0.350 | 0.122 |
| final: defaults, clustered | 6ad5c574-ec03-46e3-9eb4-4dceb3051bb5 | 0.359 | 0.126 |

A full refresh of a day of data costs 0.12 to 0.19 USD at list price. Cost follows
update duration (E and the final run were also the shortest), so the same caution
applies: E and final were back to back and may have reused warm compute. The 48
batches of run B cost 3% more than one batch, the same as the duration.

The same pipeline also billed 1.685 DBUs (0.590 USD) from 21:00 UTC with no update id
and no maintenance id. It is real cost of running the pipeline, but billing does not
say what it was for.

These runs belong to the dev pipeline that was deleted on 2026-10-01 when `gl-cicd`
took ownership, so they are found with that pipeline's old id
(`75bb9baf-ffe1-49e4-8869-28c924a7057e`) as `:pipeline_id`.

The first prod update (44944bdb-f3f8-493e-97bf-49ceae69574d, 2026-10-01 15:19 UTC) was
not in billing yet as of 2026-10-01.

**What an unpaused prod would cost:** the schedule makes 48 pipeline updates a day. At
the checkpoint 2 to 5 figure of about 0.10 USD each that is about 4.80 USD a day, before
the 96 freshness checks. This is an estimate from small runs and will be replaced with
the measured prod figure.

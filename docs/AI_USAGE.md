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

## Wrong information_schema column names in governance_check

**Produced:** `tests/integration/governance_check.py` queried `column_masks` for
`schema_name`, `mask_schema` and `mask_name`, and `row_filters` for `schema_name`,
`filter_schema`, `filter_name` and `filter_col_usage`.

**Wrong:** I took the column names from a summarized version of the docs pages and did
not check them against the workspace. CLAUDE.md says to verify APIs and never guess
parameters. A summary of a docs page is not verification. The real views use
`table_catalog`, `table_schema` and `table_name`, and neither one has a column for the
function's schema. `column_masks` has `column_name` and `mask_name`. `row_filters` has
`filter_name` and `target_columns`.

**Caught by:** Anuj's first `governance_check` run on dev:
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... schema_name cannot be resolved. Did you mean
... table_schema ...`. The error's list of candidates gave the `column_masks` columns.
`DESCRIBE gl_dev.information_schema.row_filters`, run by Anuj, gave the `row_filters`
ones.

**Fix:** the queries use the column names from the workspace. The function names are
compared whether they come back bare, schema qualified or fully qualified, so the check
does not depend on which form the views return. Which form they actually return was not
recorded. On a mismatch the check prints the actual value.

**Second failure, same check:** after the column fix, the check failed on 7 tables it
was never meant to see. It required a classification tag on every table in silver and
gold, on the assumption that those schemas hold only the published tables. They also
hold the pipeline's `__materialization_mat_*` backing tables and its `event_log_*`
table. The effective permissions API showed that no group has any privilege on them, so
the check now skips them by name. ARCHITECTURE.md records the evidence. The toggle test
also reads two of them directly as the test user.

**Also in this checkpoint:** my first version of `04_grants.sql` used
`ALTER FUNCTION ... OWNER TO`, which I could not find in the SQL reference when I
checked. I replaced it with `GRANT EXECUTE` before handing the files over, so no run
caught it.

## Predicted that a range replay would reach AUTO CDC

**Produced:** the checkpoint 5 design for `backfill_replay` range mode. ARCHITECTURE.md
and RUNBOOK.md said the silver dedupe "only covers an hour of ingest", so re-delivered
copies would get through it and reach AUTO CDC, and every rejected event would get a
second row in `quarantine_events`. The dev replay was meant to settle what AUTO CDC
does with a (key, lsn) it has already applied.

**Wrong:** I treated the one hour watermark as wall-clock time. It advances with the
`_ingested_at` of new data. In the dev run the originals had been ingested less than an
hour before the newest data, so the dedupe still held their state and dropped the
copies. The copies never reached AUTO CDC, and that run did not answer the question it
was meant to answer.

**Caught by:** the quarantine reason counts in `silver_state_check`, pasted back by
Anuj. After the replay `unparseable_payload` went from 1 to 2, while `amount_positive`
and `currency_known` did not change. Unparseable lines skip the dedupe and rejected
events go through it, so only the dedupe explains the difference. The silver and gold
checksums were unchanged, which on its own would have looked like a full pass.

**Fix:** ARCHITECTURE.md, RUNBOOK.md and the `silver_state_check` comment now describe
both cases. A follow-up replay, run 2 hours 48 minutes after the full refresh and after
two rounds of new data, did reach AUTO CDC: the rejected reason counts rose by exactly
the older files' rejected events. Silver and gold were unchanged, so AUTO CDC ignores
a (key, lsn) it has already applied. The results are in ARCHITECTURE.md.

## Shared dev deployment planned under development mode

**Produced:** the checkpoint 6 plan, which proposed a shared `root_path` for the dev
target so that CI (as `gl-cicd`) and Anuj's laptop deploy the same dev pipeline and
jobs, while keeping `mode: development`.

**Wrong:** development mode refuses that. It requires the root path to be under `~/`
or to contain the deployer's user name, and the name prefix to contain the deployer's
short name. Both exist so that two people never share one development deployment,
which is exactly what the plan wanted.

**Caught by:** `databricks bundle validate -t dev`, before anything was deployed:
`root_path must start with '~/' or contain the current username to ensure uniqueness
when using 'mode: development'`.

**Fix:** the dev target drops `mode: development` and sets the presets it needs:
`name_prefix: "[dev] "` and `pipelines_development: true`. Schedules were already
paused through `schedule_pause`. The state moved to `gl-cicd`'s home folder rather
than `/Workspace/Shared`, which validate flagged as writable by every workspace user.

## SCD2 reference opened a version for every event

**Produced:** `expected_scd2` in `src/transforms/cdc.py`, the reference history that
`silver_state_check` compares `silver.account_history` against. Every non-delete event
opened a version, and the next event closed it.

**Wrong:** two cases AUTO CDC handles differently, neither of which the checkpoint 2 to
5 runs (about 500 events) happened to contain:

- An update whose values equal the open version's. AUTO CDC opens a version only when a
  tracked column changes, so it keeps one version where the reference had two.
- A second delete in a row for the same key. AUTO CDC writes a closed row from the
  delete's before image with a NULL `__START_AT`. The reference wrote nothing.

**Caught by:** `silver_state_check` after the first full refresh on the checkpoint 6
medium dataset (1.1M events): `silver.account_history expected=215015 actual=214556
missing=940 extra=481`. A one-off diagnostic printed the events and both histories for
the differing keys. After the first fix, 14 extra rows remained, each on a key with two
deletes in a row.

**Fix:** unit tests for both cases in `tests/unit/test_scd2.py`, then the reference drops
unchanged updates and writes the repeated delete row. The pipeline was not changed; it
was right. After the fix, `silver_state_check` passed: 214,556 rows, 0 missing, 0 extra.
The repeated delete behavior is observed, not documented, so the reference models only
the case that occurred.

## CI workflows authenticated only through environment variables

**Produced:** the checkpoint 6 workflows set `DATABRICKS_AUTH_TYPE: github-oidc`,
`DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID` as environment variables, as in the
Databricks GitHub Actions example. Before pushing, I had simulated CI only with a
dummy token, which confirmed that the CLI falls back to environment variables when
the `gl-dev` profile is missing, but not which auth type it then uses.

**Wrong:** two things.

- `databricks.yml` names a profile for each target. With a profile named, CLI 1.8.0
  ignores `DATABRICKS_AUTH_TYPE` from the environment, so no auth method was chosen.
- Even with the auth type fixed, the CLI requests the GitHub token with the workspace
  token endpoint as audience. The federation policy lists the `gl-cicd` application
  ID as its audience, so the exchange would have been refused next.

**Caught by:** the first pull request. `validate` failed with `default auth: cannot
configure default credentials`, and the config it printed had `profile=gl-dev` and no
auth type. A local run with the same variables reproduced it inside the repo but not
outside it. The audience was found in the same local runs: the failed token request
URL ended in `audience=https://<workspace>/oidc/v1/token`. The Go SDK's
`determineAudience` confirmed the default.

**Fix:** `.github/scripts/databricks-profiles.sh` writes the `gl-dev` and `gl-prod`
profiles on the runner with `auth_type = github-oidc`, `client_id` and `audience`, from
repository variables. Checked locally with a fake token URL: both targets now request
the token through `github-oidc` with the configured audience.

## Federation policy subject in the wrong format

**Produced:** the subjects I gave Anuj for the `gl-cicd` federation policies,
`repo:anjmhrjn/gl-cdc-lakehouse:environment:dev` and `...:prod`, copied from the
format in the Databricks GitHub Actions example.

**Wrong:** the tokens GitHub issues for this repository carry the numeric owner and
repository IDs in the subject:
`repo:anjmhrjn@57608084/gl-cdc-lakehouse@1396470167:environment:dev`. An exact match
against the name-only form fails.

**Caught by:** the second run of `pr.yml`. The token exchange returned
`TOKEN_SUBJECT_INVALID` and printed the subject the token carried.

**Fix:** the policies use the subject GitHub actually sends. ARCHITECTURE.md and
RUNBOOK.md list it, and the RUNBOOK says where the refusal message shows the real
subject and audience.

## Shared dev deployment left job ownership to whoever deployed

**Produced:** the checkpoint 6 dev target: one deployment shared by CI and laptops,
with `CAN_MANAGE` for `gl_engineers` and `gl-cicd` and no `run_as`. I expected each
resource to keep its first owner, Anuj, and to keep running as him.

**Wrong:** on every deploy the CLI makes the deploying identity `IS_OWNER` of each job
and pipeline, unless the config names an owner. My own deploys during the experiments
ran as Anuj, so nothing changed hands and the problem stayed hidden.

**Caught by:** the first `deploy-dev` run after the merge: `cannot update
resources.jobs.governance.permissions ... only workspace admins can change the owner of
a job (403 PERMISSION_DENIED)`. `FixPermissions` in the CLI v1.8.0 source showed why.

**Fix:** with Anuj's choice, `gl-cicd` owns and runs dev as in prod. The dev target
names `gl-cicd` as `IS_OWNER` on every resource and sets `run_as`. The move needed three
more permissions, each found from a refused call: the Service Principal User role for
Anuj's laptop deploy, metastore admin to change a pipeline owner (so the dev pipeline
was deleted and recreated by CI instead), and group-only transfers for function
ownership (so the functions went to `gl_engineers`).

## Prod root path depended on who ran the command

**Produced:** the prod target's `root_path` as
`/Workspace/Users/${workspace.current_user.userName}/.bundle/...`, kept from before prod
moved to CI.

**Wrong:** CI deploys prod as `gl-cicd`, so the state is in its home. On a laptop the
same setting resolves to the laptop user's home, where there is no state, so
`bundle run -t prod` could not find the jobs CI had deployed.

**Caught by:** planning the first prod run, before any command failed. The prod run
used `pipelines start-update` and `jobs run-now` with the IDs instead.

**Fix:** prod's `root_path` names `gl-cicd`'s home. For CI that is the same path, so
nothing moved; `bundle summary -t prod` from a laptop now lists the prod resources.

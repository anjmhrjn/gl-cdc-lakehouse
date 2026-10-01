# Runbook

How to operate the pipeline: replaying data, rebuilding tables, and what to do when an
alert fires. Every command takes `-t dev` or `-t prod`. The examples use dev.

## Before any deploy

Failure emails go to the `alert_email` bundle variable. It has no default, so the
address stays out of the repo. Set it in the shell before `validate` or `deploy`:

```
export BUNDLE_VAR_alert_email=you@example.com
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## What runs when

| Job | Schedule | Retries | Fails when |
|---|---|---|---|
| `gl_pipeline_scheduled` | every 30 min | pipeline task: 2, a minute apart | the pipeline update fails, or the trial balance check fails |
| `trial_balance_check` | after every scheduled update | none | a trial balance gap is not explained by a late account, or an orphan line is older than 48h |
| `freshness_check` | every 15 min, at :05, :20, :35, :50 | none | a landed file has waited more than 60 min for a gold refresh |
| `backfill_replay` | by hand | none | any step fails, including `governance_check` at the end |
| `governance`, `governance_check` | by hand, and inside `backfill_replay` | governance: 2 | a tag, mask, filter or grant is missing |

Every job emails `alert_email` on failure. The pipeline itself emails on update failure
and on a failed flow.

Schedules are deployed paused. Development mode pauses them in dev anyway. In prod:

```
databricks bundle deploy -t prod --var schedule_pause=UNPAUSED   # start
databricks bundle deploy -t prod                                 # pause again
```

An unpaused prod runs 48 pipeline updates and 96 freshness checks a day, each on
serverless compute, whether or not anything landed.

Retries are set in `resources/reliability.yml` and have only been checked as
configuration. A transient failure cannot be caused on purpose, so no run has shown a
retry happening.

## Replay a date range

Use this when the landing files for some days have to go through the pipeline again,
for example after a bug fix in silver that should be proven on real data, or to show
that a re-delivery changes nothing.

It copies every original landing file whose `dt=` partition falls in the range into the
current partition, as `replay-<job run id>-<original name>`. Auto Loader tracks files by
path, so it ingests the copies as new files. That is a re-delivery of events silver has
already applied. Earlier `replay-*` copies are never copied again.

1. Pause the schedule in prod (see above). The job starts its own pipeline update, and
   the API refuses one while another is running.
2. Record the current state:

   ```
   databricks bundle run silver_state_check -t dev
   ```

3. Run the replay. `from` and `to` are landing dates, `YYYY-MM-DD`, inclusive. `from`
   can be at most 5 days back; the job refuses older ranges, because a re-delivered
   event could re-insert a row whose delete tombstone has expired. Use a full refresh
   for those.

   ```
   databricks bundle run backfill_replay -t dev --params mode=range,from=2026-09-30,to=2026-09-30
   ```

   The job copies the files, runs a pipeline update, reapplies governance and runs
   `governance_check`. The replay task prints how many files it re-delivered.

4. Check the result:

   ```
   databricks bundle run silver_state_check -t dev
   ```

   Every table should show `missing=0 extra=0`. The silver and gold checksums should
   equal the ones from step 2. `quarantine_events` is a log of deliveries, so its row
   count can grow: re-delivered unparseable lines always add rows, and rejected events
   add rows once their originals are older than the silver dedupe window (see
   ARCHITECTURE.md). It is compared as a set of distinct rows, so it still matches.

If the replay task fails after it started copying, do not re-run the same job run.
Start a new run. The copies that were already made carry the old run id and will be
ingested by the next update anyway. They are the same events, so silver is unchanged.

## Full refresh

Use this to rebuild tables from bronze, or everything from the landing files.

```
databricks bundle run backfill_replay -t dev --params mode=refresh,tables=all
databricks bundle run backfill_replay -t dev --params "mode=refresh,tables=silver.accounts silver.account_history"
```

- `all` resets every table, bronze included, and re-reads every landing file.
- A space separated list of `schema.table` names resets only those, then runs a normal
  update so gold catches up. Not commas: `--params` already uses commas to separate
  parameters.
- Bronze cannot be in a list. The silver streams read bronze from a checkpoint, and
  resetting bronze under them breaks those streams. Use `all` instead.

The job always reapplies governance and runs `governance_check` afterwards, because a
refresh can recreate a table without its masks and row filter. Check the result with
`silver_state_check` as in the replay steps. After a full refresh the checksums should
equal the ones before it.

## When freshness_check fails

The email subject names `freshness_check_<env>`. The run output says when gold last
refreshed and how long the oldest uncovered file has waited.

1. Look at the latest `gl_pipeline_scheduled` runs. If they failed, see "When the
   pipeline fails".
2. If the schedule is paused, freshness_check is paused too, so this should not happen.
   Check that both were deployed with the same `schedule_pause`.
3. If updates succeed but take longer than about 30 minutes, the SLA cannot hold. Look
   at the update's duration in the pipeline UI.
4. Once a pipeline update completes, run the check again:

   ```
   databricks bundle run freshness_check -t dev
   ```

## When trial_balance_check fails

The output lists every group whose net is not zero, marked either `UNBALANCED` or
`explained by late account`, and then any stale orphan lines.

- **An UNBALANCED group.** Debits and credits disagree for a business unit, currency and
  day, and no late account explains it. Find the journals behind it:

  ```sql
  SELECT journal_id, sum(CASE side WHEN 'DEBIT' THEN amount ELSE -amount END) AS net
  FROM gl_dev.silver.journal_entries
  WHERE entry_date = '<date>' AND currency = '<currency>'
  GROUP BY journal_id HAVING net != 0;
  ```

  If every journal nets to zero, the lines of one journal sit in different business
  units. That is a source problem, so report it upstream. Replaying does not fix it.
- **Stale orphan lines.** Journal lines whose account has not landed after 48 hours.
  Check whether the account's create event is in the landing files. If it landed but
  was quarantined, `silver.quarantine_events` has it. If it never landed, the source
  has to send it.

The check runs as the job owner, who sees every business unit. Do not change the job to
run as an analyst: the row filter would hide groups, and the check would pass on
partial data.

## When the pipeline fails

The pipeline emails on update failure and on flow failure. The scheduled job retries
the pipeline task twice, a minute apart, before it fails.

1. Open the failed update in the pipeline UI and read the error on the failed flow.
2. If the cause looks transient and the retries also failed, start one update by hand:

   ```
   databricks bundle run gl_pipeline -t dev
   ```

3. A data problem does not fail the pipeline. Bad rows are dropped or quarantined, and
   the counts are in the event log. So a failure is either a code or schema change, or
   infrastructure: storage credential, KMS key, or permissions.

## Deploying

Deploys go through CI. Nobody deploys prod from a laptop.

| Event | Workflow | What it does |
|---|---|---|
| pull request | `pr.yml` | ruff, unit tests, `bundle validate` for dev and prod |
| merge to main | `deploy-dev.yml` | `bundle deploy -t dev` |
| tag `v*` | `deploy-prod.yml` | `bundle validate -t prod`, `bundle deploy -t prod` |

A deploy starts no compute. Schedules stay paused unless `schedule_pause` is set to
`UNPAUSED`.

### Release to prod

1. Merge to main and check that `deploy-dev` passed.
2. Tag the commit and push the tag:

   ```
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. Check that `deploy-prod` passed in the GitHub Actions tab.
4. On the first release only, run the pipeline and then governance, because the
   governance job needs the tables to exist. Everything runs as `gl-cicd`. Running a
   prod job from a laptop needs permission to run it; Anuj has it as workspace admin:

   ```
   databricks bundle run gl_pipeline -t prod
   databricks bundle run governance -t prod
   databricks bundle run governance_check -t prod
   ```

### A deploy failed

- **`cannot configure default credentials`:** the profiles step did not run or a
  repository variable is empty. It needs `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and
  `DATABRICKS_TOKEN_AUDIENCE`.
- **Token exchange refused** (401 or 403 from `github-oidc`): the token does not match
  the federation policy. The policy subject must match the job's environment exactly
  (`repo:anjmhrjn@57608084/gl-cdc-lakehouse@1396470167:environment:dev` or
  `...:prod`), and the policy audience must equal `DATABRICKS_TOKEN_AUDIENCE`. The
  refusal message prints the subject and audience the token carried, under "Valid
  federation policy for provided token".
- **Deployment lock held:** another deploy of the same target is running, or one
  crashed. Wait for it. If it crashed, deploy once with `--force-lock` from a laptop.
- **Validation error:** fix it in a pull request. `pr.yml` runs the same validation.

A failed deploy can leave some resources updated and some not. Deploy again from the
same commit, or from the previous tag to go back.

### Laptop deploys to dev

Dev is one deployment shared by CI and laptops (see ARCHITECTURE.md). A laptop deploy
of a branch replaces what main put there until the next merge. It needs the Service
Principal User role on `gl-cicd`, because everything in dev runs as `gl-cicd`.

### Adding a job

Add the job's key to the dev target's `resources.jobs` list in `databricks.yml`, with
`{permissions: *cicd_owner}`. Without it the first deploy makes the deployer the job's
owner, and the next deploy by anyone else fails with `403 PERMISSION_DENIED`.

## Maintenance

Predictive optimization runs OPTIMIZE and VACUUM in the background. To compact at once,
for example after a large backfill:

```
databricks bundle run maintenance -t dev
databricks bundle run maintenance -t dev --params tables=silver.journal_entries,vacuum=false
```

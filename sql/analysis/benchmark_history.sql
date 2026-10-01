-- Files and bytes read by each tuning_probe benchmark query, one row per query per run.
--
-- Run by a person in the SQL editor or a notebook, not by a job: system tables are
-- readable by people only, not by gl-cicd. Set :job_id to the tuning_probe job id
-- (`databricks bundle summary -t dev` lists it).
--
-- For a serverless Python job, query history stores the Python source line that ran a
-- query, not its SQL. So the benchmark queries are found by the job id and the line
-- `row = spark.sql(sql).first()` in src/jobs/tuning_probe.py, and named by their order
-- within the run, which follows BENCHMARKS there. Records can take up to an hour to
-- appear.
SELECT
  min(start_time) OVER (PARTITION BY query_source.job_info.job_run_id) AS run_started,
  element_at(
    map(1, 'cold_account_day', 2, 'cold_account', 3, 'hot_account_day', 4, 'one_day'),
    row_number() OVER (PARTITION BY query_source.job_info.job_run_id ORDER BY start_time)
  ) AS query,
  read_bytes,
  read_files,
  pruned_files,
  total_duration_ms
FROM system.query.history
WHERE query_source.job_info.job_id = :job_id
  AND statement_text LIKE '%row = spark.sql(sql).first()%'
  AND start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY run_started, query;

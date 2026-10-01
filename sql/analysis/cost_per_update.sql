-- DBUs and list price per pipeline update, for docs/results.md.
--
-- Run by a person in the SQL editor or a notebook, not by a job: system tables are
-- readable by people only, not by gl-cicd. Set :pipeline_id to the pipeline id
-- (`databricks bundle summary -t dev` or `-t prod` lists it).
--
-- Billing records arrive hours after the usage. Prices are list prices
-- (pricing.effective_list.default), not what the account is invoiced. Usage with no
-- update id is real pipeline cost that billing does not attribute to an update.
WITH priced AS (
  SELECT u.*, u.usage_quantity * p.pricing.effective_list.default AS list_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.cloud = p.cloud
   AND u.usage_unit = p.usage_unit
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_date >= current_date() - INTERVAL 14 DAYS
)
SELECT
  usage_metadata.dlt_update_id AS update_id,
  sku_name,
  min(usage_start_time) AS first_usage,
  round(sum(usage_quantity), 3) AS dbus,
  round(sum(list_usd), 3) AS list_usd
FROM priced
WHERE usage_metadata.dlt_pipeline_id = :pipeline_id
GROUP BY ALL
ORDER BY first_usage DESC;

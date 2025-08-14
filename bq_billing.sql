#standardSQL
WITH bq_usage AS (
  SELECT
    cost,
    sku.description   AS sku_desc,
    service.description AS service_desc,
    usage_start_time
  FROM
    `project_id.utils.gcp_billing_export_resource_v1_01AE5D_EC7FC1_F2560x`
  WHERE
    -- only BigQuery
    service.description = 'BigQuery'
    -- adjust your time window as needed
    AND usage_start_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                             AND CURRENT_TIMESTAMP()
)

SELECT
  activity,
  ROUND(SUM(cost), 2) AS total_cost_usd
FROM (
  SELECT
    cost,
    CASE
      WHEN LOWER(sku_desc) LIKE '%analysis%'            THEN 'Query (Analysis)'
      WHEN LOWER(sku_desc) LIKE '%storage%'             THEN 'Storage'
      WHEN LOWER(sku_desc) LIKE '%streaming insert%'    THEN 'Streaming Inserts'
      WHEN LOWER(sku_desc) LIKE '%load job%'            THEN 'Load Jobs'
      WHEN LOWER(sku_desc) LIKE '%copy%'                THEN 'Copy Jobs'
      ELSE 'Other: ' || sku_desc
    END AS activity
  FROM bq_usage
)
GROUP BY activity
ORDER BY total_cost_usd DESC;

-- service use by user in a project/region; especially useful with ondemand pricing
SELECT
  user_email,
  SUM(total_bytes_processed) AS total_bytes_processed,
  COUNT(job_id) AS total_queries,
FROM
  `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  job_type = 'QUERY'
  AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND CURRENT_TIMESTAMP()
GROUP BY
  user_email
ORDER BY
  total_bytes_processed DESC
LIMIT 10;

-- get an table storage info
SELECT
  table_schema,
  table_name,
  total_logical_bytes,
  total_physical_bytes,
IF
  (total_logical_bytes = 0, 0, (1-ROUND(total_physical_bytes/total_logical_bytes, 2)))*100 AS compression_ratio,
  active_logical_bytes,
  long_term_logical_bytes,
 IF
  (total_logical_bytes = 0, 0, (1-ROUND(active_logical_bytes/total_logical_bytes, 2)))*100 AS long_term_ratio,
FROM
  `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT`
ORDER BY
  compression_ratio DESC;

-- find queries doing a full scan; good for when you should be using partitions
SELECT
  query,
  user_email,
  total_bytes_processed
FROM
  `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  job_type = 'QUERY'
  AND statement_type = 'SELECT'
  AND total_bytes_processed > 0
  AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) AND CURRENT_TIMESTAMP()
  AND NOT REGEXP_CONTAINS(query, r'WHERE')
ORDER BY
  total_bytes_processed DESC
LIMIT 10;

-- find tables not accessed in the last 90 days
  -- 1) Pull the last query timestamp per table
WITH
  recent_access AS (
  SELECT
    rt.project_id AS project_id,
    rt.dataset_id AS dataset_id,
    rt.table_id AS table_name,
    MAX(j.creation_time) AS last_access_time
  FROM
    `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS j
  CROSS JOIN
    UNNEST(j.referenced_tables) AS rt
  WHERE
    j.job_type = "QUERY"
    AND j.state = "DONE"
  GROUP BY
    project_id,
    dataset_id,
    table_name )
  -- 2) List all tables in the project and left-join access info
SELECT
  t.table_catalog AS project_id,
  t.table_schema AS dataset_id,
  t.table_name,
  r.last_access_time
FROM
  `region-us.INFORMATION_SCHEMA.TABLES` AS t
LEFT JOIN
  recent_access AS r
ON
  t.table_catalog = r.project_id
  AND t.table_schema = r.dataset_id
  AND t.table_name = r.table_name
  -- 3) Filter to those not accessed in the last 90 days (or never accessed)
WHERE
  COALESCE(r.last_access_time, TIMESTAMP '1970-01-01') < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
ORDER BY
  r.last_access_time;

-- slot usage by job; useful when using computre resources billing
SELECT
  creation_time,
  job_id,
  user_email,
  total_slot_ms,
FROM
  `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP()
  AND total_slot_ms > 0
ORDER BY
  total_slot_ms DESC;

-- creating a report of all view definitions
SELECT
  table_schema,
  table_name,
  view_definition
FROM
  `region-us.INFORMATION_SCHEMA.VIEWS`
ORDER BY
  table_schema,
  table_name;
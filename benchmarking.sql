SELECT
  job_id,
  start_time,
  end_time,
  total_slot_ms / 1000 AS slot_time_sec,
  query,
  job_type,
  state,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_sec,  -- Duration in seconds
  total_bytes_processed,
  total_bytes_billed,
  error_result,
FROM
  `cpsc531-project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  state = 'DONE'
  AND start_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP()
ORDER BY
  start_time DESC
LIMIT 1000;

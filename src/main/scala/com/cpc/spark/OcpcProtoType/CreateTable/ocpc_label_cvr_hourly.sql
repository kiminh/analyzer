CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_label_cvr_hourly
(
    searchid        string,
    label           int
)
PARTITIONED by (`date` STRING, `hour` STRING, cvr_goal STRING)
STORED as PARQUET;



--SELECT
--    distinct searchid
--FROM
--    dl_cpc.ocpc_label_cvr_hourly
--WHERE
--    `date` = '2019-05-31'
--AND
--    cvr_goal in ('api', 'registration', 'activation')
--
--
--SELECT
--    unitid,
--    cvr_goal
--FROM

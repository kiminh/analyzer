CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_cali_detail_report_hourly
(
    identifier              string,
    userid                  int,
    conversion_goal         int,
    cali_value              double,
    cali_pcvr               double,
    cali_postcvr            double,
    smooth_factor           double,
    cpa_suggest             double
)
PARTITIONED by (`date` STRING, `hour` STRING, version STRING)
STORED as PARQUET;
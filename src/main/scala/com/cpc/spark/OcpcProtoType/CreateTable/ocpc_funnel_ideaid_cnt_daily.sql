CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_funnel_ideaid_cnt_daily
(
    industry                string,
    ideaid_cnt              bigint,
    unitid_cnt              bigint,
    userid_cnt              bigint,
    ideaid_over_unitid      double,
    ideaid_over_userid      double
)
PARTITIONED by (`date` string)
STORED as PARQUET;
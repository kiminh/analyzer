create table if not exists dl_cpc.ocpc_aa_pre_ad_info(
    unitid              int,
    userid              int,
    conversion_goal     int
)
partitioned by (`date` string, version string)
stored as parquet;
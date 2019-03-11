create table if not exists dl_cpc.ocpc_aa_yesterday_ad_info(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    `date`                  string
)
partitioned by (dt string, version string)
stored as parquet;
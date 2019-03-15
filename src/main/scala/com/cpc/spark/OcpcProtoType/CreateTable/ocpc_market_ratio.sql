create table if not exists dl_cpc.ocpc_market_ratio(
    all_ratio               double,
    elds_ratio              double,
    feedapp_ratio           double,
    wz_ratio                double
)
partitioned by (`date` string, version string)
stored as parquet;
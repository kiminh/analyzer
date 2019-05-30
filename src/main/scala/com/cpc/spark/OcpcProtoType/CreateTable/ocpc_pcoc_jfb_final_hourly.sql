create table if not exists dl_cpc.ocpc_pcoc_jfb_final_hourly(
    identifier          string,
    pcoc                double,
    jfb                 double,
    post_cvr            double,
    conversion_goal     int
)
partitioned by (`date` string, `hour` string, version String)
stored as parquet;
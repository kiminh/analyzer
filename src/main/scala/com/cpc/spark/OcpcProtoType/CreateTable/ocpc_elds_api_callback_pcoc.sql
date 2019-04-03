create table if not exists dl_cpc.ocpc_elds_api_callback_pcoc(
    unitid          int,
    pre_cvr         double,
    post_cvr        double,
    pcoc            double
)
partitioned by(`date` string, type string)
stored as parquet;
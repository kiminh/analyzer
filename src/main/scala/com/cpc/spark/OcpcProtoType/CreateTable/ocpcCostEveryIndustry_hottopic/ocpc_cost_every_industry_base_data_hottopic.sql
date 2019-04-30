drop table if exists dl_cpc.ocpc_cost_every_industry_base_data_hottopic;
create table if not exists dl_cpc.ocpc_cost_every_industry_base_data_hottopic(
    dt                  string,
    unitid              int,
    is_ocpc             int,
    industry            string,
    siteid              int,
    is_api_callback     int,
    click               int,
    show                int,
    cost                double
)
partitioned by (`date` string)
stored as parquet;
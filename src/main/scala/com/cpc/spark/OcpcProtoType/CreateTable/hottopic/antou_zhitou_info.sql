drop table if exists dl_cpc.hottopic_antou_zhitou_info;
create table if not exists dl_cpc.hottopic_antou_zhitou_info(
    type                    string,
    ocpc_cost               double,
    ocpc_show               int,
    ocpc_cpm                double,
    all_cost                double,
    all_show                int,
    all_cpm                 double,
    ratio                   double
)
partitioned by (`date` string)
stored as parquet;
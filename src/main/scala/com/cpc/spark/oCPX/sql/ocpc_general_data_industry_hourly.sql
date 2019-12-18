create table if not exists test.ocpc_general_data_industry_hourly(
    industry                string,
    cost                    double,
    cost_cmp                double,
    cost_ratio              double,
    cost_low                double,
    cost_high               double,
    unitid_cnt              bigint,
    userid_cnt              bigint,
    low_unit_percent        double,
    pay_percent             double,
    conversion_goal         int,
    media                   string
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

alter table dl_cpc.ocpc_general_data_industry_hourly add columns (ocpc_expand int);
alter table dl_cpc.ocpc_general_data_industry_hourly add columns (pre_cvr double);
alter table dl_cpc.ocpc_general_data_industry_hourly add columns (post_cvr double);
alter table dl_cpc.ocpc_general_data_industry_hourly add columns (auc double);
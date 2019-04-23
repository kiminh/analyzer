CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_general_data_industry
(
    industry                string,
    cost                    double,
    cost_cmp                double,
    cost_ratio              double,
    cost_low                double,
    cost_high               double,
    unitid_cnt              bigint,
    userid_cnt              bigint,
    low_unit_percent        double,
    pay_percent             double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

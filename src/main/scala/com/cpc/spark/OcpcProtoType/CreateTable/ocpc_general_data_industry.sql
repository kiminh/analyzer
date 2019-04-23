CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_general_data_industry
(
    industry                string
    cost                    bigint
    high_cost               double
    low_unitid_cnt          bigint
    unitid_cnt              bigint
    userid_cnt              bigint
    low_cost                double
    cost                    bigint
    cost_cmp                double
    cost_ratio              double
    cost_low                double
    cost_high               double
    low_unit_percent        double
    pay_percent             double
)
PARTITIONED by (conversion_goal int, `date` string, `hour` string, version string, method string)
STORED as PARQUET;


industry                string
ocpc_cost               bigint
high_cost               double
low_unitid_cnt          bigint
unitid_cnt              bigint
userid_cnt              bigint
low_cost                double
cost                    bigint
cost_cmp                double
cost_ratio              double
cost_low                double
cost_high               double
low_unit_percent        double
pay_percent             double
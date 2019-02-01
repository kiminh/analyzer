CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_regression_middle_hourly_v2
(
    identifier          string,
    k_ratio             double,
    cpagiven            double,
    cpa                 double,
    ratio               double,
    click_cnt           bigint,
    cvr_cnt             bigint
)
PARTITIONED by (conversion_goal int, `date` string, `hour` string, version string)
STORED as PARQUET;


--identifier              string
--k_ratio                 double
--cpagiven                double
--cpa                     double
--ratio                   double
--click_cnt               bigint
--cvr_cnt                 bigint
--conversion_goal         int
--
--# Partition Information
--# col_name              data_type               comment
--
--date                    string
--hour                    string
--version                 string
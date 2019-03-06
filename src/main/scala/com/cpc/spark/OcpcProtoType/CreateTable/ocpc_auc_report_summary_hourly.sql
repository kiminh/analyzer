CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_auc_report_summary_hourly
(
    conversion_goal         int,
    pre_cvr                 double,
    post_cvr                double,
    q_factor                int,
    cpagiven                double,
    cpareal                 double,
    acp                     double,
    acb                     double,
    auc                     double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;


--conversion_goal         int
--pre_cvr                 double
--post_cvr                decimal(38,19)
--q_factor                int
--cpagiven                double
--cpareal                 decimal(38,19)
--acp                     decimal(38,19)
--acb                     double
--auc                     double
--date                    string
--hour                    string
--version                 string
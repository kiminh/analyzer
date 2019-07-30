CREATE TABLE IF NOT EXISTS test.ocpc_report_base_hourly
(
    ideaid                  int,
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    industry                string,
    media                   string,
    show                    bigint,
    click                   bigint,
    cv                      bigint,
    total_price             double,
    total_bid               double,
    total_precvr            double,
    total_prectr            double,
    total_cpagiven          double,
    total_jfbfactor         double,
    total_cvrfactor         double,
    total_calipcvr          double,
    total_calipostcvr       double,
    total_cpasuggest        double,
    total_smooth_factor     double,
    is_hidden               int
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;
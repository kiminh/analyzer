CREATE TABLE test.ocpc_pcoc_pred_diff_by_userid_hourly(
    userid                  int,
    conversion_goal         int,
    pcoc                    double,
    current_pcoc            double,
    current_cv              bigint
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;
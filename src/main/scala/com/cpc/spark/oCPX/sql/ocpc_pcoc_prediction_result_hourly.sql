CREATE TABLE test.ocpc_pcoc_prediction_result_hourly(
    identifier                  string,
    media                       string,
    conversion_goal             int,
    conversion_from             int,
    time                        string,
    hour_diff                   int,
    double_feature_list         array<double>,
    string_feature_list         array<string>,
    avg_pcoc                    double,
    pred_pcoc                   double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;
CREATE TABLE test.ocpc_pcoc_sample_hourly(
    identifier                  string,
    media                       string,
    conversion_goal             int,
    conversion_from             int,
    double_feature_list         array<double>,
    string_feature_list         array<string>,
    hour_diff                   int,
    time                        string,
    label                       double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;
CREATE TABLE test.ocpc_pcoc_sample_part_hourly(
    identifier                  string,
    media                       string,
    conversion_goal             int,
    conversion_from             int,
    double_feature_list         array<double>,
    string_feature_list         array<string>
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;
CREATE TABLE dl_cpc.ocpc_pcoc_sample_part1_hourly(
    identifier                  string,
    media                       string,
    conversion_goal             int,
    conversion_from             int,
    feature_list                array<double>
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;
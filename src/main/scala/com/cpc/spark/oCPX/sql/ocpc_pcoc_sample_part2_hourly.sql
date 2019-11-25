CREATE TABLE test.ocpc_pcoc_sample_part2_hourly(
    identifier                  string,
    media                       string,
    conversion_goal             int,
    conversion_from             int,
    pcoc                        double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;
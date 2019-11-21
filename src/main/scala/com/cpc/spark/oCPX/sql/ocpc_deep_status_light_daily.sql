create table if not exists test.ocpc_deep_status_light_version(
    identifier              string,
    flag                    int
)
partitioned by (version string)
stored as parquet;



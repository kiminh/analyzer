create table if not exists test.ocpc_auto_second_stage_light(
    unitid                  int,
    userid                  int,
    media                   string,
    conversion_goal         int,
    ocpc_status             int,
    adclass                 int,
    industry                string,
    cost_flag               int,
    time_flag               int,
    flag_ratio              int,
    random_value            int,
    user_black_flag         int,
    user_cost_flag          int,
    unit_white_flag         int,
    flag                    int
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

create table if not exists test.ocpc_union_slow_log(
    searchid                string,
    unitid                  int,
    media                   string,
    conversion_goal         int,
    industry                string,
    bid                     int,
    price                   int,
    exp_cvr                 double,
    isclick                 int,
    iscvr                   int
)
partitioned by (`date` string, `hour` string)
stored as parquet;
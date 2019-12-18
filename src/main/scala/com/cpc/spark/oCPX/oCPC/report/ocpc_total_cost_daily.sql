create table if not exists test.ocpc_total_cost_daily(
    unitid                  int,
    userid                  int,
    usertype                int,
    adslot_type             int,
    media                   string,
    adclass                 int,
    ocpc_step               int,
    show                    bigint,
    click                   bigint,
    cost                    double
)
partitioned by(`date` string)
stored as parquet;
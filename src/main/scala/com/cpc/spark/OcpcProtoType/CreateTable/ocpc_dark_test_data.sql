create table if not exists dl_cpc.ocpc_dark_test_data(
    unitid                  int,
    userid                  int,
    ab_group                string,
    conversion_goal         int,
    acp                     double,
    cpm                     double,
    cpagiven                double,
    cpareal                 double,
    pre_cvr                 double,
    post_cvr                double,
    cost                    double,
    show                    int,
    click                   int,
    cv                      int
)
partitioned by (`date` string, version string)
stored as parquet;
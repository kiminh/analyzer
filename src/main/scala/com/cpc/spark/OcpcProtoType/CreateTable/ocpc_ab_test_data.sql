create table if not exists dl_cpc.ocpc_ab_test_data(
    dt                      string,
    unitid                  int,
    userid                  int,
    ab_group                string,
    acp                     double,
    acb                     double,
    acb_max                 double,
    cpm                     double,
    cpagiven                double,
    cpareal                 double,
    pre_cvr                 double,
    post_cvr                double,
    kvalue                  double,
    cost                    double,
    show                    bigint,
    click                   bigint,
    cv                      bigint
)
partitioned by (`date` string, tag string, version string)
stored as parquet;

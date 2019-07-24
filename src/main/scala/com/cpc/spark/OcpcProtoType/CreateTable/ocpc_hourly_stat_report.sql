CREATE TABLE IF NOT EXISTS test.ocpc_hourly_stat_report
(
    unitid                  int,
    planid                  int,
    userid                  int,
    adclass                 int,
    industry                string,
    adslot_type             int,
    conversion_goal         int,
    media                   string,
    click                   bigint,
    show                    bigint,
    cv                      bigint,
    cost                    double,
    cpagiven                double,
    cpareal                 double,
    acp                     double,
    acb                     double,
    ctr                     double,

)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;


create table dl_cpc.ocpc_hourly_stat_report
like test.ocpc_hourly_stat_report;
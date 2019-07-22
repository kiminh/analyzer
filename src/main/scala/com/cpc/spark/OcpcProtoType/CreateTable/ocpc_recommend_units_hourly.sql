CREATE TABLE IF NOT EXISTS test.ocpc_recommend_units_hourly
(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    media                   string,
    adclass                 int,
    industry                string,
    usertype                int,
    adslot_type             int,
    show                    bigint,
    click                   bigint,
    cvrcnt                  bigint,
    cost                    bigint,
    post_ctr                double,
    acp                     double,
    acb                     double,
    jfb                     double,
    cpa                     double,
    pre_cvr                 double,
    post_cvr                double,
    pcoc                    double,
    cal_bid                 double,
    auc                     double,
    is_recommend            int
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

create table dl_cpc.ocpc_recommend_units_hourly
like test.ocpc_recommend_units_hourly;


alter table test.ocpc_recommend_units_hourly add columns (ocpc_status int)
alter table dl_cpc.ocpc_recommend_units_hourly add columns (ocpc_status int)

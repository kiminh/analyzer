CREATE TABLE IF NOT EXISTS test.ocpc_recommend_units_hourly
(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    media                   string,
    adclass                 int,
    industry                string,
    usertype                int,
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


--"unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pre_cvr", "post_cvr", "pcoc", "cal_bid", "auc", "is_recommend"
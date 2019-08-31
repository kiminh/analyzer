create table if not exists test.ocpc_report_industry_hourly(
    media                   string,
    industry                string,
    conversion_goal         int,
    is_hidden               int,
    show                    bigint,
    click                   bigint,
    cv                      bigint,
    exp_cpm                 double,
    pre_ctr                 double,
    post_ctr                double,
    pre_cvr                 double,
    cali_precvr             double,
    post_cvr                double,
    cost                    double,
    pay                     double,
    buffer                  double,
    acp                     double,
    acb                     double,
    cpagiven                double,
    jfb_factor              double,
    cvr_factor              double,
    cali_postcvr            double,
    smooth_factor           double
)
partitioned by(`date` string, `hour` string)
stored as parquet;
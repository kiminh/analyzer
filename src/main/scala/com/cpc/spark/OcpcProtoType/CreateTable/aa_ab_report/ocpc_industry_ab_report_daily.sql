drop table if exists dl_cpc.ocpc_industry_ab_report_daily;
create table if not exists dl_cpc.ocpc_industry_ab_report_daily(
    industry                string,
    type                    string,
    cv                      int,
    click                   int,
    show                    int,
    cost                    double,
    cpm                     double,
    pre_cvr                 double,
    post_cvr                double,
    cost_of_every_click     double,
    bid_of_every_click      double,
    cpa_given               double,
    cpa_real                double,
    arpu                    double
)
partitioned by(`date` string, version string)
stored as parquet;
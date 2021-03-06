drop table if exists dl_cpc.ocpc_unit_aa_report_hourly;
create table if not exists dl_cpc.ocpc_unit_aa_report_hourly(
    unitid                              int,
    userid                              int,
    industry                            string,
    put_type                            double,
    adslot_type                         int,
    conversion_goal                     int,
    cv                                  int,
    click                               int,
    show                                int,
    bid                                 double,
    cost                                double,
    cpm                                 double,
    exp_cvr                             double,
    pre_cvr                             double,
    post_cvr                            double,
    cost_of_every_click                 double,
    cpa_real                            double,
    cpa_given                           double,
    hidden_cost_ratio                   double,
    kvalue                              double,
    budget                              double,
    cost_budget_ratio                   double,
    ocpc_cpc_cpm_ratio                  double,
    ocpc_cpc_pre_cvr_ratio              double,
    ocpc_cpc_post_cvr_ratio             double,
    ocpc_cpc_cost_of_every_click        double,
    ocpc_cpc_bid_of_every_click         double,
    ocpc_cpc_cpa_real                   double
)
partitioned by(`date` string, hour string, version string)
stored as parquet;
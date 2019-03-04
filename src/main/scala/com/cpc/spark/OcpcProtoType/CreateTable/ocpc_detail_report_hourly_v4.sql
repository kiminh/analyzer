CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_detail_report_hourly_v4
(
    identifier              string,
    userid                  int,
    conversion_goal         int,
    step2_click_percent     double,
    is_step2                int,
    cpa_given               double,
    cpa_real                double,
    cpa_ratio               double,
    is_cpa_ok               int,
    impression              bigint,
    click                   bigint,
    conversion              bigint,
    ctr                     double,
    click_cvr               double,
    show_cvr                double,
    cost                    double,
    acp                     double,
    avg_k                   double,
    recent_k                double,
    pre_cvr                 double,
    post_cvr                double,
    q_factor                int,
    acb                     double,
    auc                     double
)
PARTITIONED by (`date` STRING, `hour` STRING, version STRING)
STORED as PARQUET;
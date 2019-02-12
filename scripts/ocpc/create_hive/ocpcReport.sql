
CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_detail_report_hourly_v2
(
    user_id                 int,
    idea_id                 int,
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





CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_summary_report_hourly_v2
(
    conversion_goal         int,
    total_adnum             bigint,
    step2_adnum             bigint,
    low_cpa_adnum           bigint,
    high_cpa_adnum          bigint,
    step2_cost              double,
    step2_cpa_high_cost     double,
    impression              bigint,
    click                   bigint,
    conversion              bigint,
    ctr                     double,
    click_cvr               double,
    cost                    double,
    acp                     double,
    pre_cvr                 double,
    post_cvr                double,
    q_factor                int,
    acb                     double,
    auc                     double
)
PARTITIONED by (`date` STRING, `hour` STRING, version STRING)
STORED as PARQUET;
CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_summary_report_hourly_v4
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
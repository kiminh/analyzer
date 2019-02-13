CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_unionlog (
    searchid string,
    timestamp bigint,
    uid string,
    exp_ctr float,
    exp_cvr float,
    ideaid int,
    price int,
    userid int,
    adclass int,
    isclick int,
    isshow int,
    exptags string,
    cpa_given int,
    ocpc_log string,
    iscvr int,
    ocpc_log_dict map<string, string>
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dl_cpc.dssm_eval_raw (
  uid string,
  ideaid int,
  clickCount bigint,
  score double,
  userNull int,
  adNull int,
  userEmbedding string,
  adEmbedding string
)
PARTITIONED BY (dt string)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dl_cpc.slim_unionlog (
    searchid string,
    ts bigint,
    uid string,
    exp_ctr float,
    ideaid int,
    bid int,
    price int,
    userid int,
    adclass int,
    isclick int,
    isshow int,
    exptags string,
    adslot_type int,
    bs_rank_tag string,
    embeddingNum bigint,
    bs_num int
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET;

---------------------

CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_detail_report_hourly_v3
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





CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_summary_report_hourly_v3
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

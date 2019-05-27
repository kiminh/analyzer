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

CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_post_cvr_unitid_hourly_novel (
    identifier string,
    min_bid double,
    cvr1 double,
    cvr2 double,
    cvr3 double,
    cvr4 double,
    min_cpm double,
    factor1 double,
    factor2 double,
    factor3 double,
    factor4 double,
    cpc_bid double,
    cpa_suggest int,
    param_t int,
    cali_value double
)
PARTITIONED BY (date string, hour string, version string)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dl_cpc.recall_filter_number_report_v3
(
    searchid string,
    group_media_num int,
    group_region_num int,
    group_l_v_num int,
    group_os_type_num int,
    group_p_l_num int,
    group_interest_num int,
    group_acc_user_type_num int,
    group_new_user_num int,
    group_content_category_num int,
    group_black_install_pkg_num int,
    group_white_install_pkg_num int,
    group_show_count_num int,
    group_click_count_num int,
    matched_group_num int,
    involved_group_num int,
    len_groups int,
    involved_idea_num int,
    matched_idea_num int,
    rnd_idea_num int,
    exptags string,
    adslot_type string,
    req_io_time int,
    process_time int,
    ad_slot_id string,
    media_class string,
    hostname string,
    group_age_num int,
    group_gender_num int,
    group_network_num int,
    group_ad_slot_type_num int,
    group_map_match_count_num int,
    groups_hit_media_ids string,
    groups_hit_age_ids string,
    groups_hit_gender_ids string,
    groups_hit_net_work_ids string,
    groups_hit_ad_slot_type_ids string,
    groups_hit_media_class_ids string,
    groups_hit_regional_ids string,
    groups_hit_user_level_ids string,
    groups_hit_phone_level_ids string,
    groups_hit_os_type_ids string,
    groups_hit_black_install_pkg_ids string,
    groups_hit_white_install_pkg_ids string,
    groups_hit_content_category_ids string,
    groups_hit_new_user_ids string,
    groups_hit_acc_user_type_ids string,
    groups_hit_interest_or_user_signal_ids string,
    groups_filtered_by_ad_show_ids string,
    groups_filtered_by_ad_click_ids string,
    groups_filtered_by_black_user_ids string,
    groups_filtered_by_black_sid_ids string,
    groups_filtered_by_not_delivery_pr_ids string,
    ideas_filtered_by_material_type_ids string,
    ideas_filtered_by_interaction_ids string,
    ideas_filtered_by_black_class_ids string,
    ideas_filtered_by_acc_class_ids string,
    ideas_filtered_by_material_level_ids string,
    ideas_filtered_by_only_site_ids string,
    ideas_filtered_by_filter_goods_ids string,
    ideas_filtered_by_chitu_ids string,
    ideas_filtered_by_pkg_filter_ids string,
    bid_avg_before_filter int,
    bid_avg_after_filter int,
    bid_avg_return_as int,
    ad_num_of_delivery int,
    media_appsid string
)
PARTITIONED by (`dt` STRING, `hour` STRING)
STORED as PARQUET;

create table if not exists dl_cpc.ocpc_novel_pb_hourly
(
identifier string,
kvalue double,
conversion_goal int,
post_cvr double,
cvrcalfactor double,
cpagiven double,
maxbid int,
smoothfactor double
)
PARTITIONED by (date STRING, hour STRING,version STRING)
STORED as PARQUET;

create table if not exists dl_cpc.ocpc_cpagiven_novel_v3_hourly
(
unitid int,
new_adclass int,
cost bigint,
cvrcnt bigint,
qtt_avgbid double,
qtt_cpa double,
maxbid double,
alpha double,
conversion_goal int,
alpha_max double,
cpa_max double,
cpagiven double,
adv_conversion_goal int,
ocpc_bid bigint
)
PARTITIONED by (date STRING, hour STRING,version STRING)
STORED as PARQUET;

create table if not exists dl_cpc.ocpc_pcoc_jfb_novel_v3_hourly
(
identifier string,
pcoc double,
jfb double,
post_cvr double

)
PARTITIONED by (date STRING, hour STRING,version STRING)
STORED as PARQUET;

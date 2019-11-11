CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_filter_unionlog
(
    searchid                    string,
    `timestamp`                int,
    network                     int,
    exptags                     string,
    media_type                  int,
    media_appsid                string,
    adslotid                    string,
    adslot_type                 int,
    adtype                      int,
    adsrc                       int,
    interaction                 int,
    bid                         int,
    price                       int,
    ideaid                      int,
    unitid                      int,
    planid                      int,
    country                     int,
    province                    int,
    city                        int,
    uid                         string,
    ua                          string,
    os                          int,
    sex                         int,
    age                         int,
    isshow                      int,
    isclick                     int,
    duration                    int,
    userid                      int,
    is_ocpc                     bigint,
    user_city                   string,
    city_level                  int,
    adclass                     int,
    ocpc_log_dict               map<string,string>
)
PARTITIONED by (`date` STRING, `hour` STRING)
STORED as PARQUET;

--alter table dl_cpc.ocpc_filter_unionlog add columns (exp_ctr double);
--alter table dl_cpc.ocpc_filter_unionlog add columns (exp_cvr double);
--alter table dl_cpc.ocpc_filter_unionlog add columns (antispam int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (conversion_goal int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (charge_type int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (conversion_from int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (is_api_callback int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (siteid int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (cvr_model_name string);
--alter table dl_cpc.ocpc_filter_unionlog add columns (user_req_ad_num int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (user_req_num int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (is_new_ad int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (is_auto_coin int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (bid_discounted_by_ad_slot bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (second_cpm bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (final_cpm bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (exp_cpm bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (ocpc_expand int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (expids string);
--alter table dl_cpc.ocpc_filter_unionlog add columns (bsctr bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (bscvr bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (raw_cvr bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (deep_cvr bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (raw_deep_cvr bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (deep_cvr_model_name string);
--alter table dl_cpc.ocpc_filter_unionlog add columns (deep_ocpc_log_dict map<string,string>);
--alter table dl_cpc.ocpc_filter_unionlog add columns (is_deep_ocpc int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (deep_conversion_goal int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (deep_cpa bigint);
--alter table dl_cpc.ocpc_filter_unionlog add columns (cpa_check_priority int);
--alter table dl_cpc.ocpc_filter_unionlog add columns (ocpc_expand_tag int)
alter table test.ocpc_filter_unionlog add columns (tuid string);
alter table test.ocpc_filter_unionlog add columns (hidden_tax int);
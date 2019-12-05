CREATE TABLE IF NOT EXISTS test.ocpc_filter_unionlog_hourly
(
    searchid                        string,
    exptags                         string,
    media_type                      int,
    media_appsid                    string,
    adslotid                        string,
    adslot_type                     int,
    adtype                          int,
    bid                             int,
    price                           int,
    ideaid                          int,
    unitid                          int,
    planid                          int,
    country                         int,
    province                        int,
    city                            int,
    uid                             string,
    os                              int,
    isshow                          int,
    isclick                         int,
    duration                        int,
    userid                          int,
    is_ocpc                         bigint,
    ocpc_log_dict                   map<string,string>,
    user_city                       string,
    city_level                      int,
    adclass                         int,
    exp_ctr                         double,
    exp_cvr                         double,
    raw_cvr                         double,
    usertype                        bigint,
    conversion_goal                 int,
    conversion_from                 int,
    is_api_callback                 int,
    cvr_model_name                  string,
    bid_discounted_by_ad_slot       bigint,
    bscvr                           int,
    ocpc_expand                     int,
    deep_cvr                        bigint,
    raw_deep_cvr                    bigint,
    deep_cvr_model_name             string,
    deep_ocpc_log_dict              map<string,string>,
    is_deep_ocpc                    int,
    deep_conversion_goal            int,
    deep_cpa                        bigint,
    cpa_check_priority              int
)
PARTITIONED by (`date` STRING, `hour` STRING)
STORED as PARQUET;


--alter table dl_cpc.ocpc_filter_unionlog_hourly add columns (hidden_tax int);
--alter table dl_cpc.ocpc_filter_unionlog_hourly add columns (pure_deep_exp_cvr int);
alter table test.ocpc_filter_unionlog_hourly add columns (deep_ocpc_step int);
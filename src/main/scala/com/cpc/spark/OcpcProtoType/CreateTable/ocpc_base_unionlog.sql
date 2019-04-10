CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_base_unionlog
(
    searchid        string,
    `timestamp`       int,
    network         int,
    exptags         string,
    media_type      int,
    media_appsid    string,
    adslotid        string,
    adslot_type     int,
    adtype          int,
    adsrc           int,
    interaction     int,
    bid             int,
    price           int,
    ideaid          int,
    unitid          int,
    planid          int,
    country         int,
    province        int,
    city            int,
    uid             string,
    ua              string,
    os              int,
    sex             int,
    age             int,
    isshow          int,
    isclick         int,
    duration        int,
    userid          int,
    is_ocpc         bigint,
    ocpc_log        string,
    user_city       string,
    city_level      int,
    adclass         int
)
PARTITIONED by (`date` STRING, `hour` STRING)
STORED as PARQUET;

--alter table dl_cpc.ocpc_base_unionlog add columns (exp_ctr double);
--alter table dl_cpc.ocpc_base_unionlog add columns (exp_cvr double);
--alter table dl_cpc.ocpc_base_unionlog add columns (charge_type int);
--alter table dl_cpc.ocpc_base_unionlog add columns (antispam int);
--alter table dl_cpc.ocpc_base_unionlog add columns (usertype bigint);
--alter table dl_cpc.ocpc_base_unionlog add columns (conversion_goal int);
--alter table dl_cpc.ocpc_base_unionlog add columns (conversion_from int);
--alter table dl_cpc.ocpc_base_unionlog add columns (is_api_callback int);
--alter table dl_cpc.ocpc_base_unionlog add columns (siteid int);
alter table dl_cpc.ocpc_base_unionlog add columns (cvr_model_name string);
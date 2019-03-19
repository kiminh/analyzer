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
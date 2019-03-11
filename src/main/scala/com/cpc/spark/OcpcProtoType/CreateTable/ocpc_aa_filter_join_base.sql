create table if not exists dl_cpc.ocpc_aa_filter_join_base(
    `date`              string,
    unitid              int,
    userid              int,
    conversion_goal     int,
    searchid            string,
    isclick             int,
    isshow              int,
    price               int,
    `uid`               string,
    ocpc_log            string,
    ocpc_log_dict       map<string, string>
)
partitioned by (dt string, version string)
stored as parquet;
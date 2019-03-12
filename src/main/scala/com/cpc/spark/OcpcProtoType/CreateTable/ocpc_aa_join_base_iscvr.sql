create table if not exists dl_cpc.ocpc_aa_join_base_iscvr(
    `date`              string,
    unitid              int,
    userid              int,
    searchid            string,
    isclick             int,
    isshow              int,
    price               int,
    uid                 string,
    iscvr1              int,
    iscvr2              int,
    iscvr3              int,
    ocpc_log            string,
    ocpc_log_dict       map<string, string>
)
partitioned by (dt string, version string)
stored as parquet;
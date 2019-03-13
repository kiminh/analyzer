create table if not exists dl_cpc.ocpc_aa_join_base_iscvr(
    unitid              int,
    userid              int,
    searchid            string,
    isclick             int,
    isshow              int,
    price               int,
    uid                 string,
    bid                 int,
    is_ocpc             int,
    iscvr1              int,
    iscvr2              int,
    iscvr3              int,
    ocpc_log            string,
    ocpc_log_dict       map<string, string>
)
partitioned by (`date` string, version string)
stored as parquet;
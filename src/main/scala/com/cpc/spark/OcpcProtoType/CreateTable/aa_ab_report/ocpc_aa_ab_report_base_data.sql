create table if not exists dl_cpc.ocpc_aa_ab_report_base_data(
    searchid                string,
    unitid                  int,
    userid                  int,
    adslot_type             int,
    industry                string,
    exptags                 string,
    is_ocpc                 int,
    isclick                 int,
    isshow                  int,
    bid                     int,
    conversion_goal         int,
    pcvr                    double,
    is_hidden               int,
    kvalue                  int,
    budget                  int,
    cpa_given               double,
    price                   int,
    exp_cvr                 double,
    iscvr                   int
)
partitioned by(`date` string, hour string, version string)
stored as parquet;
create table if not exists dl_cpc.ocpc_aa_base_index(
    `date`                  string,
    unitid                  int,
    userid                  int,
    searchid                string,
    isclick                 int,
    isshow                  int,
    price                   int,
    uid                     string,
    bid                     int,
    cpagiven                double,
    kvalue                  double,
    pcvr                    double,
    dynamicbidmax           double,
    dynamicbid              double,
    conversion_goal         int,
    iscvr1                  int,
    iscvr2                  int,
    iscvr3                  int
)
partitioned by (dt string, version string)
stored as parquet;
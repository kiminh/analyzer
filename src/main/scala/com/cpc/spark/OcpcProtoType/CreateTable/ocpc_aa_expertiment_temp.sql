create table if not exists dl_cpc.ocpc_aa_expertiment_temp(
    `date`              string,
    unitid              int,
    userid              int,
    conversion_goal     int,
    searchid            string,
    isclick             int,
    isshow              int,
    price               int,
    uid                 string,
    cpagiven            double,
    kvalue              double,
    pcvr                double,
    dynamicbidmax       double,
    dynamicbid          double,
    iscvr               int
)
partitioned by (dt string, version string)
stored as parquet;
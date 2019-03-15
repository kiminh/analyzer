CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_ab_test_temp(
    searchid                string,
    exptags                 string,
    unitid                  int,
    ideaid                  int,
    userid                  int,
    isclick                 int,
    isshow                  int,
    price                   int,
    cpagiven                double,
    kvalue                  double,
    pcvr                    double,
    dynamicbidmax           double,
    dynamicbid              double,
    conversion_goal         int,
    iscvr1                  int,
    iscvr2                  int,
    iscvr3                  int,
    iscvr                   int,
    dt                      string,
    hr                      string
)
PARTITIONED BY (`date` string, tag string, version string)
STORED AS PARQUET;
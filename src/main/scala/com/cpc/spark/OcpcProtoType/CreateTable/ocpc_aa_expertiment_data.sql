create table if not exists dl_cpc.ocpc_aa_expertiment_data(
    `dt`                    string,
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    cpagiven                double,
    cpareal                 double,
    cpm                     double,
    arpu                    double,
    show                    int,
    click                   int,
    cv                      int,
    pre_cvr                 double,
    post_cvr                double,
    acp                     double,
    acb                     double,
    kvalue                  double,
    ratio                   double
)
partitioned by (`date` string, version string)
stored as parquet;
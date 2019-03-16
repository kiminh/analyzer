create table if not exists dl_cpc.ocpc_aa_expertiment_data(
    `dt`                    string,
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    is_hidden               int,
    adslot_type             int,
    cv_goal                 int,
    cv                      int,
    click                   int,
    show                    int,
    charge                  int,
    pre_cvr                 double,
    post_cvr                double,
    acp                     double,
    acb                     double,
    cpareal                 double,
    cpagiven                double,
    suggest_cpa             double,
    auc                     double,
    kvalue                  double,
    cpm                     double
)
partitioned by (`date` string, version string)
stored as parquet;
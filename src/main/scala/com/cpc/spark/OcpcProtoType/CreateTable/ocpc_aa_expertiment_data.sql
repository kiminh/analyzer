create table if not exists dl_cpc.ocpc_aa_expertiment_data(
    `date`              string,
    unitid              int,
    userid              int,
    cpagiven            double,
    cpareal             double,
    cpm                 double,
    arpu                double,
    show                double,
    click               double,
    cv                  double,
    pre_cvr             double,
    post_cvr            double,
    acp                 double,
    acb                 double,
    ratio               double
)
partitioned by (dt string, version string)
stored as parquet;
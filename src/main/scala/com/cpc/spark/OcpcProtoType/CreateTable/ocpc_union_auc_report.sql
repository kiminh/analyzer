create table if not exists test.wt_union_auc_report(
    conversion_goal                 int,
    pre_cvr                         double,
    post_cvr                        double,
    q_factor                        int,
    cpagiven                        double,
    cpareal                         double,
    acp                             double,
    acb                             double,
    auc                             double,
    hour                            string,
    version                         string
)
partitioned by (`date` string)
stored as parquet;
drop table if exists dl_cpc.ocpc_cost_every_industry_hottopic;
create table if not exists dl_cpc.ocpc_cost_every_industry_hottopic(
    industry                    string,
    ocpc_show                   int,
    all_show                    int,
    ocpc_click                  int,
    all_click                   int,
    ocpc_cost                   double,
    all_cost                    double,
    cost_ratio                  double,
    all_cost_yesterday          double,
    all_cost_yesterday_ratio    double,
    all_cost_days7ago           double,
    all_cost_days7ago_ratio     double,
    ocpc_cost_yesterday         double,
    ocpc_cost_yesterday_ratio   double,
    ocpc_cost_days7ago          double,
    ocpc_cost_days7ago_ratio    double,
    all_unit_yesterday          int,
    all_unit_today              int,
    ocpc_unit_yesterday         int,
    ocpc_unit_today             int,
    new_ocpc_unit               int,
    recommend_unit              int
)
partitioned by (`date` string)
stored as parquet;
drop table if exists dl_cpc.ocpc_cost_every_industry_data;
create table if not exists dl_cpc.ocpc_cost_every_industry_data(
industry                    string,
all_click                   int,
ocpc_click                  int,
all_show                    int,
ocpc_show                   int,
ocpc_cost                   double,
all_cost                    double,
ratio                       double,
all_cpm                     double,
ocpc_cpm                    double
)
partitioned by (`date` string, version string)
stored as parquet;
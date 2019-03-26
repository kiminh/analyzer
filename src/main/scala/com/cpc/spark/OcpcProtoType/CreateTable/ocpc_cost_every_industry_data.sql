create table if not exists dl_cpc.ocpc_cost_every_industry_data(
industry                string,
click                   int,
show                    int,
ocpc_cost               double,
all_cost                double,
ratio                   double
)
partitioned by (`date` string, version string)
stored as parquet;
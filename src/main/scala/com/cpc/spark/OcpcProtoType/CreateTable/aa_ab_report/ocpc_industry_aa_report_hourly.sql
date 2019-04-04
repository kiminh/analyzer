drop table if exists dl_cpc.ocpc_industry_aa_report_hourly;
create table if not exists dl_cpc.ocpc_industry_aa_report_hourly(
    industry                    string,
    all_unit_num                int,
    all_user_num                int,
    ocpc_user_num               int,
    ocpc_unit_num               int,
    cv                          int,
    click                       int,
    show                        int,
    cost                        double,
    cpm                         double,
    ocpc_cost                   double,
    ocpc_cost_ratio             double,
    cpa_control_num             int,
    cpa_control_ratio           double,
    ocpc_hidden_num             int,
    ocpc_hidden_cost            double,
    ocpc_hidden_cost_ratio      double,
    hidden_control_num          int,
    hidden_control_ratio        double,
    hit_line_num                int,
    hidden_hit_line_ratio       double,
    avg_hidden_cost             double,
    avg_hidden_budget           double,
    hidden_budget_cost_ratio    double
)
partitioned by(`date` string, hour string, version string)
stored as parquet;
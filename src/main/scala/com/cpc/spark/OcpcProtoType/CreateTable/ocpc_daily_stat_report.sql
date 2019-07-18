--("unitid", "planid", "userid", "adclass", "industry", "adslot_type", "media", "click", "show", "cv", "cost", "cpa", "ocpc_click", "ocpc_show", "ocpc_cv", "ocpc_cost", "ocpc_cpagiven", "ocpc_cpareal", "budget")
CREATE TABLE IF NOT EXISTS test.ocpc_daily_stat_report
(
    unitid                  int,
    planid                  int,
    userid                  int,
    adclass                 int,
    industry                string,
    adslot_type             int,
    media                   string,
    click                   bigint,
    show                    bigint,
    cv                      bigint,
    cost                    double,
    cpa                     double,
    ocpc_click              bigint,
    ocpc_show               bigint,
    ocpc_cv                 bigint,
    ocpc_cost               double,
    ocpc_cpagiven           double,
    ocpc_cpareal            double,
    budget                  double
)
PARTITIONED by (`date` string)
STORED as PARQUET;


create table dl_cpc.ocpc_daily_stat_report
like test.ocpc_daily_stat_report;

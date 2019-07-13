CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_funnel_data_industry_daily
(
    unitid                  int,
    planid                  int,
    userid                  int,
    click                   bigint,
    show                    bigint,
    cv                      bigint,
    cost                    double,
    ocpc_cpagiven           double,
    ocpc_cpareal            double,
    ocpc_click              bigint,
    ocpc_show               bigint,
    ocpc_cv                 bigint,
    ocpc_cost               double,
    hidden_cpagiven         double,
    hidden_cpareal          double,
    hidden_click            bigint,
    hidden_show             bigint,
    hidden_cv               bigint,
    hidden_cost             double,
    budget                  double
)
PARTITIONED by (industry string, `date` string)
STORED as PARQUET;



--alter table dl_cpc.ocpc_funnel_data_industry_daily add columns (adslot_type int);
--alter table dl_cpc.ocpc_funnel_data_industry_daily add columns (site_type string);
alter table test.ocpc_funnel_data_industry_daily add columns (media string);

--adslot_type             int
--site_type               string
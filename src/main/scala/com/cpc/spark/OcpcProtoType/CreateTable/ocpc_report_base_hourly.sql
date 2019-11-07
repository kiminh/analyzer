CREATE TABLE IF NOT EXISTS test.ocpc_report_base_hourly
(
    ideaid                  int,
    unitid                  int,
    userid                  int,
    adclass                 int,
    conversion_goal         int,
    industry                string,
    media                   string,
    show                    bigint,
    click                   bigint,
    cv                      bigint,
    total_price             bigint,
    total_bid               bigint,
    total_precvr            double,
    total_prectr            double,
    total_cpagiven          bigint,
    total_jfbfactor         double,
    total_cvrfactor         double,
    total_calipcvr          double,
    total_calipostcvr       double,
    total_cpasuggest        double,
    total_smooth_factor     double,
    is_hidden               int,
    adslot_type             int
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;

alter table dl_cpc.ocpc_report_base_hourly add columns (adslot_type int);
alter table dl_cpc.ocpc_report_base_hourly add columns (total_exp_cpm double);

alter table test.ocpc_report_base_hourly add columns (total_rawcvr double);
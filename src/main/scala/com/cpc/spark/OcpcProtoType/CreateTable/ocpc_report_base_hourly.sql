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

--alter table dl_cpc.ocpc_report_base_hourly add columns (adslot_type int);
--alter table dl_cpc.ocpc_report_base_hourly add columns (total_exp_cpm double);
--alter table dl_cpc.ocpc_report_base_hourly add columns (total_rawcvr double);
--alter table dl_cpc.ocpc_report_base_hourly add columns (deep_conversion_goal int);
--alter table dl_cpc.ocpc_report_base_hourly add columns (cpa_check_priority int);
--alter table dl_cpc.ocpc_report_base_hourly add columns (is_deep_ocpc int);
alter table dl_cpc.ocpc_report_base_hourly add columns (deep_click bigint, deep_cv bigint, total_deepcvr double, total_deep_cpagiven bigint, total_deep_jfbfactor double, total_deep_cvrfactor double, total_deep_calipcvr double, total_deep_smooth_factor double, real_deep_click bigint, total_deep_price bigint, total_deep_bid bigint);
alter table dl_cpc.ocpc_report_base_hourly add columns (ocpc_expand int)

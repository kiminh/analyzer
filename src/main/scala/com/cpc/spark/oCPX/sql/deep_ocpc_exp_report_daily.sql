create table if not exists test.deep_ocpc_exp_report_daily(
    cali_tag                string,
    recall_tag              string,
    cpa_check_priority      int,
    media                   string,
    unitid                  int,
    conversion_goal         int,
    deep_conversion_goal    int,
    click                   bigint,
    cost                    double,
    pre_cvr1                double,
    pre_cvr2                double,
    cv1                     bigint,
    cv2                     bigint,
    cpagiven                double,
    deep_cpagiven           double
)
partitioned by (`date` string)
stored as parquet;

alter table dl_cpc.deep_ocpc_exp_report_daily add columns (show bigint);
alter table test.deep_ocpc_exp_report_daily add columns (deep_ocpc_step int)
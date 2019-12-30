create table if not exists test.ocpc_compensate_backup_daily(
    unitid                  int,
    userid                  int,
    compensate_key          string,
    ocpc_charge_time        string,
    cost                    double,
    conversion              int,
    cpareal                 double,
    cpagiven                double,
    pay                     double,
    cpc_flag                int,
    logic_version           int,
    create_time             string
)
partitioned by (`date` string)
stored as parquet;

alter table dl_cpc.ocpc_compensate_backup_daily add columns (is_deep_ocpc int);
alter table dl_cpc.ocpc_compensate_backup_daily add columns (deep_ocpc_charge_time string);
alter table dl_cpc.ocpc_compensate_backup_daily add columns (is_pay int);
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
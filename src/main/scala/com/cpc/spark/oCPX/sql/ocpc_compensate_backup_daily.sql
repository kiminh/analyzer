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
    create_time             string,
    is_deep_ocpc            int,
    deep_ocpc_charge_time   string,
    is_pay                  int
)
partitioned by (`date` string, version string)
stored as parquet;




create table if not exists dl_cpc.ocpc_compensate_backup_daily_v2(
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
    create_time             string,
    is_deep_ocpc            int,
    deep_ocpc_charge_time   string,
    is_pay                  int
)
partitioned by (`date` string, version string)
stored as parquet;
CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_charge_daily
(
    unitid                  int,
    cost                    bigint,
    conversion              bigint,
    pay                     double,
    ocpc_time               string,
    cpagiven                double,
    cpareal                 double

)
PARTITIONED by (`date` string, version string)
STORED as PARQUET;


-- unitid                  int
-- cost                    bigint
-- conversion              bigint
-- pay                     double
-- ocpc_time               string
-- cpagiven                double
-- cpareal                 double
-- date                    string
-- version                 string
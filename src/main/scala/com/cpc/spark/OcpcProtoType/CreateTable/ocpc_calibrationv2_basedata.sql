CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_calibrationv2_basedata
(
    unitid                  int,
    cost                    bigint,
    conversion              bigint,
    pay                     double,
    ocpc_time               string,
    cpagiven                double,
    cpareal                 double

)
PARTITIONED by (converion_goal int)
STORED as PARQUET;
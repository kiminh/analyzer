CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_kvalue_smooth_strat
(
    identifier              string,
    pcoc                    double,
    jfb                     double,
    kvalue                  double,
    conversion_goal         int
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;



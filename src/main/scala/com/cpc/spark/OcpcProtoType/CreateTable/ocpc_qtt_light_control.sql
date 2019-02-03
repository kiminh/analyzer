CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_qtt_light_control
(
    unitid          int,
    cpc_cpa         double,
    ocpc_cpa        double
)
PARTITIONED by (conversion_goal int, version STRING)
STORED as PARQUET;


--unitid  int     NULL
--cpc_cpa1        double  NULL
--cpc_cpa2        double  NULL
--cpc_cpa3        double  NULL
--ocpc_cpa1       double  NULL
--ocpc_cpa2       double  NULL
--ocpc_cpa3       double  NULL
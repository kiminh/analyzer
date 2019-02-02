CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_pb_result_hourly_v2
(
    identifier          string,
    conversion_goal     int,
    cpagiven            double,
    cvrcnt              bigint,
    kvalue              double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;


--identifier              string
--conversion_goal         int
--cpagiven                double
--cvrcnt                  bigint
--kvalue                  double
--version                 string
--
--# Partition Information
--# col_name              data_type               comment
--
--version                 string
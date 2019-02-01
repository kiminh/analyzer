CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_prev_pb_once
(
    identifier          string,
    conversion_goal     int,
    cpagiven            double,
    cvrcnt              bigint,
    kvalue              double
)
PARTITIONED by (version STRING)
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
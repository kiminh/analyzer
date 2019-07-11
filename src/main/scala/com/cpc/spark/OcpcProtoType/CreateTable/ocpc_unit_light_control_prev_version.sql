CREATE TABLE IF NOT EXISTS test.ocpc_unit_light_control_prev_version
(
    unitid                  int,
    userid                  int,
    adclass                 int,
    media                   string,
    cpa                     double,
    `date`                  string,
    `hour`                  string
)
PARTITIONED by (version string)
STORED as PARQUET;

create table dl_cpc.ocpc_unit_light_control_prev_version
like test.ocpc_unit_light_control_prev_version;
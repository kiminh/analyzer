CREATE TABLE IF NOT EXISTS test.ocpc_unit_light_control_version
(
    unitid                  int,
    userid                  int,
    adclass                 int,
    media                   string,
    cpa                     double
)
PARTITIONED by (version string)
STORED as PARQUET;

create table dl_cpc.ocpc_unit_light_control_version
like test.ocpc_unit_light_control_version;
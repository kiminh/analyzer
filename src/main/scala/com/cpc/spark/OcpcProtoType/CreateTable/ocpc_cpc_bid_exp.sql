CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_cpc_bid_exp
(
    identifier          string,
    cpc_bid             double,
    duration            int
)
PARTITIONED by (`date` string, version string)
STORED as PARQUET;
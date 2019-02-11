CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_check_min_bid_v2
(
    hr                      string,
    adslot_type             bigint,
    city_level              int,
    ad_second_class         bigint,
    ocpc_flag               int,
    min_bid                 double,
    min_cpm                 bigint,
    cnt                     bigint,
    min_cnt                 double
)
PARTITIONED by (`date` STRING, `hour` STRING, version STRING)
STORED as PARQUET;


--hr                      string
--adslot_type             bigint
--city_level              int
--ad_second_class         bigint
--ocpc_flag               int
--min_bid                 double
--min_cpm                 bigint
--cnt                     bigint
--min_cnt                 double
--date                    string
--hour                    string
--version                 string
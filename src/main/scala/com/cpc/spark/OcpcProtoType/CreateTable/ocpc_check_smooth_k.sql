CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_check_smooth_k
(
    identifier      string,
    kvalue          double,
    conversion_goal int,
    original_k      double,
    base_k          double,
    hour_cnt        bigint,
    factor          double,
    bottom_k        double,
    top_k           double,
    flag            int
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

--identifier      string  NULL
--kvalue  double  NULL
--conversion_goal int     NULL
--original_k      double  NULL
--base_k  double  NULL
--hour_cnt        bigint  NULL
--factor  double  NULL
--bottom_k        double  NULL
--top_k   double  NULL
--flag    int     NULL
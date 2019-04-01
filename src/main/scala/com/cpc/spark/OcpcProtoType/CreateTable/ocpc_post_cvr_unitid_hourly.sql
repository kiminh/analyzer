create table if not exists dl_cpc.ocpc_aa_base_index(
    identifier              string,
    min_bid                 double,
    cvr1                    double,
    cvr2                    double,
    cvr3                    double,
    min_cpm                 double,
    factor1                 double,
    factor2                 double,
    factor3                 double,
    cpc_bid                 double,
    cpa_suggest             double,
    param_t                 double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;





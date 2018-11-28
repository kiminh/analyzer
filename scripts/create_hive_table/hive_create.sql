CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_unionlog (
    searchid string,
    timestamp bigint,
    uid string,
    exp_ctr float,
    exp_cvr float,
    ideaid int,
    price int,
    userid int,
    adclass int,
    isclick int,
    isshow int,
    exptags string,
    cpa_given int,
    ocpc_log string,
    iscvr int,
    ocpc_log_dict map<string, string>
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dl_cpc.dssm_eval_raw (
  uid string,
  ideaid int,
  clickCount bigint,
  score double,
  userNull int,
  adNull int,
  userEmbedding string,
  adEmbedding string
)
PARTITIONED BY (dt string)
STORED AS PARQUET;



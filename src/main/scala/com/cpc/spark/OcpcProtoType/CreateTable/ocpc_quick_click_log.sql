CREATE TABLE IF NOT EXISTS test.ocpc_quick_click_log
(
    searchid                string,
    unitid                  int,
    userid                  int,
    adslot_type             int,
    conversion_goal         int,
    media                   string,
    industry                string,
    isclick                 int,
    exp_cvr                 double
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;

alter table dl_cpc.ocpc_quick_click_log add columns (ocpc_step int);
alter table dl_cpc.ocpc_quick_click_log add columns (adclass int);

alter table dl_cpc.ocpc_quick_click_log add columns (price int);
alter table dl_cpc.ocpc_quick_click_log add columns (adtype int);
CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_suggest_raw_data
(
    searchid                string,
    unitid                  int,
    userid                  int,
    adclass                 int,
    isshow                  int,
    isclick                 int,
    price                   int,
    bid                     int,
    ocpc_log                string,
    industry                string,
    usertype                bigint,
    exp_cvr                 double,
    exp_ctr                 double,
    ocpc_log_dict           map<string,string>,
    iscvr                   int
)
PARTITIONED by (conversion_goal int, version string)
STORED as PARQUET;

--searchid                string
--unitid                  int
--userid                  int
--adclass                 int
--isshow                  int
--isclick                 int
--price                   int
--bid                     int
--ocpc_log                string
--industry                string
--usertype                bigint
--exp_cvr                 double
--exp_ctr                 double
--ocpc_log_dict           map<string,string>
--iscvr                   int
--conversion_goal         int
--version                 string
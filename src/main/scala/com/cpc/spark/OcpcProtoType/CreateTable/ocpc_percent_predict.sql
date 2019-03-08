create table if not exists dl_cpc.ocpc_predict_percent(
    unitid                  int,
    userid                  int,
    is_recommend            int,
    ocpc_flat               int,
    acb                     double,
    kvalue                  double,
    cal_bid                 double,
    zerobid_percent         double,
    bottom_halfbid_percent  double,
    top_halfbid_percent     double,
    largebid_percent        double,
    predict_percent         double
)
partitioned by (`date` string, version string)
stored as parquet;


--CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_predict_percent(
--	unitid                      int,
--	userid                      int,
--	is_recommend                int,
--	ocpc_flag                   int,
--	acb                         double,
--	kvalue                      double,
--	cal_bid                     double,
--	zerobid_percent             double,
--	bottom_halfbid_percent      double,
--	top_halfbid_percent         double,
--	largebid_percent            double,
--	percent                     double
--)
--PARTITIONED BY (`date` string, version string)
--STORED AS PARQUET;
package com.cpc.spark.ml.calibration

import org.apache.spark.sql.SparkSession


object CalibrationClickLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // parse and process input
    val date = args(0)

    // get union log
    val sql = s"""
                 |select a.*,b.conversion_target[0] as unit_target, if(c.searchid is null,0,1) iscvr
                 |from
                 |(
                 |  select searchid, raw_cvr, exp_cvr, cvr_model_name, adslot_id as adslotid, ideaid, user_req_ad_num,day as dt,hour,unitid,adclass,
                 |  case
                 |  when is_ocpc = 1 then 'ocpc'
                 |  when user_cvr_threshold = 200 then "cvr2"
                 |  when user_cvr_threshold >0 then "cvr1"
                 |  else "other"
                 |  end as exp_cvr_type
                 | from dl_cpc.cpc_basedata_union_events
                 | where day = '$date'
                 | and media_appsid in ('80000001', '80000002') and isclick = 1
                 | and cvr_model_name in ('qtt-cvr-dnn-rawid-v1-180','qtt-cvr-dnn-rawid-v1-180-newcali')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
                 | )a
                 |join dl_cpc.dw_unitid_detail b
                 |on a.unitid = b.unitid and b.day = '$date'
                 |left join dl_cpc.dm_conversion_detail c
                 |on a.searchid = c.searchid and c.dt = '$date'
       """.stripMargin
    println(s"sql:\n$sql")
    val sample = spark.sql(sql).filter("unit_target is not null and unit_target not in ('none','site_uncertain')")
      .select("searchid","raw_cvr","cvr_model_name","adslotid","ideaid","user_req_ad_num","iscvr","exp_cvr","adclass","exp_cvr_type","dt","hour")

    sample.show(10)
    sample.repartition(1).write.mode("overwrite").insertInto("dl_cpc.qtt_cvr_calibration_sample")
  }
}
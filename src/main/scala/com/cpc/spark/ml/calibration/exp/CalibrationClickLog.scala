package com.cpc.spark.ml.calibration.exp

import org.apache.spark.sql.SparkSession

object CalibrationClickLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // parse and process input
    val date = "2019-10-10"
    val endHour = 23
    // get union log
    val sql = s"""
                 |select a.searchid, cast(b.raw_cvr as bigint) as rawcvr, substring(a.adclass,1,6) as adclass,
                 |b.cvr_model_name as model, b.adslot_id as adslotid, a.ideaid,user_show_ad_num, exp_cvr,
                 |unitid,userid,click_count,click_unit_count,
                 |if(c.iscvr is not null,1,0) iscvr,if(hour>$endHour,hour-$endHour,hour+24-$endHour) hourweight
                 |from
                 |(select searchid,ideaid,unitid,userid,adclass,hour
                 |  from dl_cpc.cpc_basedata_click_event
                 |  where day = '$date'
                 |  and media_appsid in ('80000001','80000002') and isclick = 1
                 |  and adsrc in (1,28)
                 |  and antispam_score = 10000
                 |  )a
                 |  join
                 |  (select searchid,ideaid,user_show_ad_num,conversion_goal,raw_cvr,cvr_model_name,adslot_id,exp_cvr
                 |  ,click_count,click_unit_count
                 |  from
                 |  dl_cpc.cpc_basedata_adx_event
                 |  where  day = '$date'
                 |  and media_appsid in ('80000001','80000002')
                 |  and cvr_model_name = "qtt-cvr-dnn-rawid-v1wzjf-aibox"
                 |  AND bid_mode = 0
                 |  and conversion_goal>0) b
                 |    on a.searchid = b.searchid and a.ideaid = b.ideaid
                 | left join
                 | (select distinct searchid,conversion_goal,1 as iscvr
                 |  from dl_cpc.ocpc_quick_cv_log
                 |  where  `date` = '$date') c
                 |  on a.searchid = c.searchid and b.conversion_goal=c.conversion_goal
       """.stripMargin
    println(s"sql:\n$sql")
    val sample = spark.sql(sql)
    sample.show()
    sample.write.mode("overwrite").saveAsTable("dl_cpc.wy_calibration_sample_2019_10_10")
  }
}

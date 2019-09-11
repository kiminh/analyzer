package com.cpc.spark.ml.recall.dyrec

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object dyrec_feature_daily {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dyrec_feature")
      .enableHiveSupport()
      .getOrCreate()
    val curday = args(0)
    val cal1 = Calendar.getInstance()
    cal1.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -6)
    val sevenday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    val sample = spark.sql(
      s"""
         |select * from (select uid,slotid9, slotid10, slotid11, slotid12,slotid13, slotid14,slotid15, slotid16,slotid17, slotid18,
         |slotid19, slotid20,slotid21, slotid143,slotid206,slotid207,slotid208, slotid209,slotid210, slotid211,slotid212,slotid213,
         |slotid214,slotid215, slotid219,slotid220, slotid221,slotid222, slotid223,slotid224, slotid225,
         |row_number() over(partition by uid order by day,hour desc) as row_num from dl_cpc.recall_rec_feature where day>='$sevenday') t1
         |where row_num=1
       """.stripMargin)
    val uid_memberid = spark.sql(
      s"""
         |select member_id as uid,device_code as uid_ad from gobblin.qukan_member_info_incre
         |where member_id > 0 and device_code is not null group by member_id,device_code
      """.stripMargin)

    sample.join(uid_memberid, Seq("uid"), "left_outer").createOrReplaceTempView("sample_new")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.recall_rec_feature_daily partition (day="$oneday")
         |select COALESCE(uid_ad, uid) as uid_new, slotid9, slotid10, slotid11, slotid12,slotid13, slotid14,slotid15, slotid16,slotid17, slotid18,
         |slotid19, slotid20,slotid21, slotid143,slotid206,slotid207,slotid208, slotid209,slotid210, slotid211,slotid212,slotid213,
         |slotid214,slotid215, slotid219,slotid220, slotid221,slotid222, slotid223,slotid224, slotid225 from sample_new
         |""".stripMargin)
    spark.sql(
      s"""
         |insert into dl_cpc.recall_rec_feature_successday
         |select '$oneday'
         |""".stripMargin)
  }
}

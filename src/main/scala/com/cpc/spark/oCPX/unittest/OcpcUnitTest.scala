package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeSchedule.{getTodayData, udfDeterminePayIndustry}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString

    println("parameters:")
    println(s"date=$date, hour=$hour")

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adslot_type,
         |  adclass,
         |  conversion_goal,
         |  timestamp,
         |  from_unixtime(timestamp,'YYYY-MM-dd HH:mm:ss') as ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  date = '$date'
         |""".stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("industry", udfDeterminePayIndustry()(col("adslot_type"), col("adclass"), col("conversion_goal")))
      .filter(s"industry in ('feedapp', 'elds', 'pay_industry', 'siteform_pay_industry')")
      .filter(s"seq = 1")
      .select("unitid", "timestamp", "ocpc_charge_time")

    data1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200113a")



  }

}



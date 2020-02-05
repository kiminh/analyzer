package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.light_control.suggest_cpa.OcpcLightBulb.{getRecommendationAd, getUnitData}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = "ocpctest"

    println("parameters:")
    println(s"date=$date, hour=$hour")


    // 抽取推荐cpa数据和白名单单元
    val suggestUnits = getRecommendationAd(version, date, hour, spark)

    // 检查线上的媒体id进行校验
    val units = getUnitData(spark)


    suggestUnits
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20200205a")

    units
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20200205b")



  }

}



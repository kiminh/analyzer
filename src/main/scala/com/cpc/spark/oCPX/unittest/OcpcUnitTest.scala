package com.cpc.spark.oCPX.unittest


import com.cpc.spark.OcpcProtoType.bs.OcpcBsData.getBaseData
import com.cpc.spark.oCPX.oCPC.light_control.white_list.OcpcFreePass.getUnitData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.oCPC.light_control.white_list.OcpcFreePass._
import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeCost.{assemblyData, getDeepData, getShallowData}
import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeSchedule.{getOcpcCompensate, getTodayData}


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

    // 计算七天的分天展点消以及浅层转化
    val shallowOcpcData = getShallowData(date, 7, spark)

    // 计算七天的分天展点消以及深层转化
    val deepOcpcData = getDeepData(date, 7, spark)

    // 数据关联
    val data = assemblyData(shallowOcpcData, deepOcpcData, spark)

    data
      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191211c")

  }

}



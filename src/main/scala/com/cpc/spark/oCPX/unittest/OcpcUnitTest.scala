package com.cpc.spark.oCPX.unittest


import com.cpc.spark.OcpcProtoType.bs.OcpcBsData.getBaseData
import com.cpc.spark.oCPX.oCPC.light_control.white_list.OcpcFreePass.getUnitData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.oCPC.light_control.white_list.OcpcFreePass._
import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeCost.{assemblyData, calculatePayRaw, getDeepData, getSchedule, getShallowData}
import com.cpc.spark.oCPX.oCPC.pay.v2.OcpcChargeSchedule.{getOcpcCompensate, getTodayData, joinSchedule, updateSchedule}


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

//    // 获取前一天的ocpc_compensate线上数据（备份表），基于ocpc_charge_time和deep_ocpc_charge_time来判断周期开始日期以及分别需要计算深度还是浅层赔付
//    val ocpcCompensate = getOcpcCompensate(date, 7, spark)
//
//    // 统计今天的分单元消耗和开始消费时间
//    val todayData = getTodayData(date, spark)
//
//    // 更新赔付周期表
//    val data = joinSchedule(ocpcCompensate, todayData, spark)
//
//    data
//      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191211a")
//
//    // 更新赔付周期表
//    val result = updateSchedule(data, date, 7, spark)
//
//    result
//      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191211b")

    // 计算七天的分天展点消以及浅层转化
    val shallowOcpcData = getShallowData(date, 7, spark)

//    // 计算七天的分天展点消以及深层转化
//    val deepOcpcData = getDeepData(date, 7, spark)
//
//    // 数据关联
//    val data = assemblyData(shallowOcpcData, deepOcpcData, spark)
//
//    // 抽取周期数据表
//    val scheduleData = getSchedule(date, spark)
//
//    // 统计消费与赔付
//    val payDataRaw = calculatePayRaw(data, scheduleData, date, spark)

    shallowOcpcData
      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191211d")

//    deepOcpcData
//      .write.mode("overwrite").saveAsTable("test.ocpc_check_exp_data20191211e")

  }

}



package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.udfConcatStringInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcChargeCost {
  def main(args: Array[String]): Unit = {
    /*
    按照七天周期计算赔付数据
    1. 计算当天所有单元的点击、消费、转化、平均cpagiven、平均cpareal、赔付金额
    2. 获取这批单元在赔付周期中的起始时间
    3. 如果当前为周期第一天，则重新落表，否则，叠加上前一天的历史数据
    4. 数据落表，需包括周期编号，是否周期第一天
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val dayCnt = args(1).toInt

    // 计算七天的分天展点消以及浅层和深层转化


  }


}

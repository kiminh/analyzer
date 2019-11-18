package com.cpc.spark.oCPX.oCPC.report

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{SparkSession}
import com.cpc.spark.oCPX.oCPC.report.OcpcHourlyReport._


object OcpcHourlyPreviousReport {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyPreviousReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val dayInt = args(1).toInt
    println("parameters:")
    println(s"date=$date")

    for (dayCnt <- 1 to dayInt) {
      // 取历史数据
      val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
      val today = dateConverter.parse(date)
      val calendar = Calendar.getInstance
      calendar.setTime(today)
      calendar.add(Calendar.HOUR, -dayCnt)
      val yesterday = calendar.getTime
      val date1 = dateConverter.format(yesterday)

      println("###############################################")
      println(s"update report data: $date, $dayCnt, $date1")
//      updateData(date1, spark)
    }
  }

  def updateData(date: String, spark: SparkSession) = {
    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, "23", spark)

    // 分ideaid和conversion_goal统计数据
    val baseData = calculateBaseData(rawData, spark)

    // 深度转化数据
    val deepRawData = getDeepBaseData(date, "23", spark)

    // 统计深度转化指标
    val deepBaseData = calculateDeepBaseData(deepRawData, spark)

    // 数据关联
    val data = baseData
      .join(deepBaseData, Seq("ideaid", "unitid", "userid", "adclass", "adslot_type", "conversion_goal", "deep_conversion_goal", "cpa_check_priority", "is_deep_ocpc", "industry", "media", "hr", "is_hidden"), "left_outer")

    // 存储数据到hadoop
    saveBaseDataToHDFS(data, date, "23", spark)
  }
}
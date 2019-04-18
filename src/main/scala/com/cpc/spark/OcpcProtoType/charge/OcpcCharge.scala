package com.cpc.spark.OcpcProtoType.charge

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCharge {
  def main(args: Array[String]): Unit = {
    /*
    根据最近四天有投放oCPC广告的广告单元各自的消费时间段的消费数据统计是否超成本和赔付数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val dayCnt = args(4).toInt

    val ocpcOpenTime = getOcpcOpenTime(date, hour, spark)

    val baseData = getOcpcData(media, dayCnt, date, hour, spark)

    val correctData = filterData(baseData, ocpcOpenTime, date, hour, spark)

  }

  def filterData(baseData: DataFrame, ocpcOpenTime: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val rawData = baseData
      .join(ocpcOpenTime, Seq("unitid", "conversion_goal"), "inner")
      .select("searchid", "unitid", "conversion_goal", "isshow", "isclick", "price", "ocpc_last_open_date", "ocpc_last_open_hour", "date", "hour")
      .withColumn("flag", udfCmpTime()(col("date"), col("hour"), col("ocpc_last_open_date"), col("ocpc_last_open_hour")))

    rawData.write.mode("overwrite").saveAsTable("test.check_ocpc_payback_cost20190418")

  }

  def udfCmpTime() = udf((date: String, hour: String, open_date: String, open_hour: String) => {
    var flag = 0
    if (date < open_date) {
      flag = 0
    } else if (date > open_date) {
      flag = 1
    } else {
      if (hour < open_hour) {
        flag = 0
      } else {
        flag = 1
      }
    }
    flag
  })

  def getOcpcData(media: String, dayCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key1 = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key1)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  isshow,
         |  isclick,
         |  price,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick=1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).filter(s"is_hidden = 0")

    data
  }

  def getOcpcOpenTime(date: String, hour: String, spark: SparkSession) = {
    /*
    从dl_cpc.ocpc_unit_list_hourly抽取每个单元最后一次打开oCPC的时间
     */
    // 抽取最后打开时间
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    conversion_goal,
         |    ocpc_last_open_date,
         |    ocpc_last_open_hour
         |FROM
         |    dl_cpc.ocpc_unit_list_hourly
         |WHERE
         |    `date` = '$date'
         |AND
         |    `hour` = '$hour'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data

  }
}
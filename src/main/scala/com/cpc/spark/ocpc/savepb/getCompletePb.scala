package com.cpc.spark.ocpc.savepb

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.ocpc.utils.OcpcUtils._

object getCompletePb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    saveFinalDataIntoPb(date, hour, spark)
  }

  def saveFinalDataIntoPb(date: String, hour: String, spark: SparkSession) = {
//    test.new_pb_ocpc_with_pcvr_complete_bak2
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  round(k_value * 1000.0) as k
         |FROM
         |  test.new_pb_ocpc_with_pcvr_complete_bak2
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.show(10)

    // get the k value from historical data
    val historyK = checkKpercent(date, hour, 2, spark)

    // merge the data
    val data = rawData
      .join(historyK, Seq("ideaid", "adclass", "k"), "left_outer")
      .select("ideaid", "adclass", "k", "flag")
    println("################## final result #####################")
    data.show(10)


  }

  def checkKpercent(date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |select
         |  ideaid,
         |  adclass,
         |  round(ocpc_log_dict['kvalue'] * 1000.0) as k,
         |  count(1) as cnt
         |from
         |  dl_cpc.ocpc_unionlog
         |WHERE
         |  $selectCondition2
         |AND
         |  isclick=1
         |group by ideaid, adclass, round(ocpc_log_dict['kvalue'] * 1000.0)
       """.stripMargin
    println(sqlRequest)

    val data = spark.sql(sqlRequest).withColumn("flag", lit(1))
    data.show(10)
    data


  }


}

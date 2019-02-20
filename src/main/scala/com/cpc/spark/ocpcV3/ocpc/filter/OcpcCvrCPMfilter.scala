package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcCvrCPMfilter {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile")
    println(s"path is: $expDataPath")
    println(s"fileName is: $fileName")

    val data = getCPMdata(date, hour, spark)
    data.show(10)

    val resultDF = data
      .filter("unitid is not null")
      .withColumn("identifier", col("unitid"))
      .select("identifier", "min_bid", "min_cpm")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pcvr_smooth_cpm")
  }


  def getCPMdata(date: String, hour: String, spark: SparkSession) = {
    val clickData = getClickData(date, hour, spark)
    clickData.write.mode("overwrite").saveAsTable("test.ocpc_check_data_smooth_cpm")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  1.5 as min_bid2,
         |  percentile(cpm, 0.10) as min_cpm2
         |FROM
         |  test.ocpc_check_data_smooth_cpm
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF

  }

  def getClickData(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  price,
         |  isclick,
         |  isshow,
         |  (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['dynamicbid'] as double)
         |        else original_bid end) as bid,
         |  exp_ctr * 1000000 as exp_ctr
         |FROM
         |  dl_cpc.filtered_union_log_exptag_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ('80000001', '80000002')
         |AND
         |  adslot_type in (1,2,3)
         |AND
         |  adsrc=1
         |AND
         |  round(adclass/1000) != 132101  --去掉互动导流
         |AND
         |  isshow=1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).withColumn("cpm", col("bid") * col("exp_ctr"))

    data
  }
}
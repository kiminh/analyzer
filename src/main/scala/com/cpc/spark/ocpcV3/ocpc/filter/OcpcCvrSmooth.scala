package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.Ocpccpcbid
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcCvrSmooth {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
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

    val cvrData = getCvrData(date, hour, spark)
    cvrData.show(10)

    val resultDF = cvrData
      .filter("unitid is not null")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))
      .withColumn("identifier", col("unitid"))
      .select("identifier", "cvr1", "cvr2", "cvr3")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

//    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pcvr_smooth_hourly")
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pcvr_smooth_hourly")

  }


  def getCvrData(date: String, hour: String, spark: SparkSession) = {
    val clickData = getClickData(date, hour, spark)
    val cvr1Data = getCV("cvr1", date, hour, spark)
    val cvr2Data = getCV("cvr2", date, hour, spark)
    val cvr3Data = getCV("cvr3", date, hour, spark)

    val data = clickData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "price", "isclick", "iscvr1", "iscvr2", "iscvr3", "isshow", "bid", "exp_ctr")
      .withColumn("cpm", col("bid") * col("exp_ctr"))

    // data.createOrReplaceTempView("base_data")
    data.write.mode("overwrite").saveAsTable("test.ocpc_check_data_smooth_cvr")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  sum(iscvr1) * 1.0 / sum(isclick) as cvr1,
         |  sum(iscvr2) * 1.0 / sum(isclick) as cvr2,
         |  sum(iscvr3) * 1.0 / sum(isclick) as cvr3
         |FROM
         |  test.ocpc_check_data_smooth_cvr
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF

  }

  def getCV(cvType: String, date: String, hour: String, spark: SparkSession) = {
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
    val colName = "is" + cvType

    val sqlRequest =
      s"""
         |SELECT
         |  searchid
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvType'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).withColumn(colName, lit(1)).select("searchid", colName)
    data
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
    val data = spark.sql(sqlRequest)

    data
  }

}
package com.cpc.spark.ocpcV3.ocpcNovel

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import ocpcnovel.ocpcnovel.SingleUnit
import ocpcnovel.ocpcnovel.OcpcNovelList
//import ocpcnovel.ocpcnovel

import scala.collection.mutable.ListBuffer


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cvrData = getCvr(date, hour, spark)
    val kvalue = getK(date, hour, spark)
    val cpaHistory = getCPAhistory(date, hour, spark)

    val data = cvrData
      .join(kvalue, Seq("unitid"), "left_outer")
      .select("unitid", "kvalue", "cvr1cnt", "cvr2cnt")
      .join(cpaHistory, Seq("unitid"), "left_outer")
      .withColumn("kvalue", when(col("kvalue").isNull, lit(0.5)).otherwise(col("kvalue")))
      .filter("cpa_history is not null")
      .select("unitid", "cpa_history", "kvalue", "cvr1cnt", "cvr2cnt")
    data.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_pb_hourly")

    // 输出pb文件
    savePbPack(data)
  }

  def getK(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpc_v3_novel_k_regression"
    val rawData = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    rawData.show(10)

    val resultDF = rawData
      .select("unitid")
      .distinct()
      .withColumn("kvalue", lit(0.5))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def getCPAhistory(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpcv3_novel_cpa_history_hourly"
    val resultDF = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    resultDF.show(10)

    resultDF
  }

  def getCvr(date: String, hour: String, spark: SparkSession) = {
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

    // cvr data
    // cvr1 or cvr3 data
    val sqlRequestCvr1Data =
    s"""
       |SELECT
       |  unitid,
       |  cvr1_cnt
       |FROM
       |  dl_cpc.ocpcv3_cvr1_data_hourly
       |WHERE
       |  $selectCondition
       |AND
       |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr1Data)
    val cvr1Data = spark
      .sql(sqlRequestCvr1Data)
      .groupBy("unitid")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
    cvr1Data.show(10)

    // cvr2data
    val sqlRequestCvr2Data =
      s"""
         |SELECT
         |  unitid,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr2Data)
    val cvr2Data = spark
      .sql(sqlRequestCvr2Data)
      .groupBy("unitid")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)

    // 数据关联
    val resultDF = cvr1Data
      .join(cvr2Data, Seq("unitid"), "outer")
      .withColumn("cvr1cnt", when(col("cvr1cnt").isNull, 0).otherwise(col("cvr1cnt")))
      .withColumn("cvr2cnt", when(col("cvr2cnt").isNull, 0).otherwise(col("cvr2cnt")))
      .select("unitid", "cvr1cnt", "cvr2cnt")

    // 返回结果
    resultDF.show(10)
    resultDF
  }

  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUnit]
    val filename = s"OcpcNovel.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    var cnt = 0

    for (record <- dataset.collect()) {
      val unitid = record.getAs[Int]("unitid").toString
      val cpaHistory = record.getAs[Double]("cpa_history")
      val kvalue = record.getAs[Double]("kvalue")
      val cvr1cnt = record.getAs[Int]("cvr1cnt")
      val cvr2cnt = record.getAs[Int]("cvr2cnt")

      if (cnt % 500 == 0) {
        println(s"unitid:$unitid, cpaHistory:$cpaHistory, kvalue:$kvalue, cvr1cnt:$cvr1cnt, cvr2cnt:$cvr1cnt")
      }
      cnt += 1

      val currentItem = SingleUnit(
        unitid = unitid,
        kvalue = kvalue,
        cpaHistory = cpaHistory,
        cvr2Cnt = cvr1cnt,
        cvr3Cnt = cvr2cnt
      )
      list += currentItem

    }
    val result = list.toArray[SingleUnit]
    val adUnitList = OcpcNovelList(
      adunit = result
    )

    println("length of the array")
    println(result.length)
    adUnitList.writeTo(new FileOutputStream(filename))

    //    dataset.write.mode("overwrite").saveAsTable("test.new_pb_ocpc_with_pcvr")
    println("complete save data into protobuffer")

  }


}

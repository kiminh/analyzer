package com.cpc.spark.oCPX.bsOcpc.assembly

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.typesafe.config.ConfigFactory
import ocpcBsParmas.ocpcBsParmas.{OcpcBsParmasList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcBsDataFinal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt
    val fileName = args(6).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt, version:$version, expTag:$expTag, minCV:$minCV, fileName:$fileName")


    val result = getData(date, hour, version, spark)


    result
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("exp_tag", lit(expTag))
        .withColumn("version", lit(version))
        .repartition(5)
//        .write.mode("overwrite").insertInto("test.ocpc_bs_params_pb_hourly")
        .write.mode("overwrite").insertInto("dl_cpc.ocpc_bs_params_pb_hourly")


    savePbPack(result, fileName, spark)

  }

  def getData(date: String, hour: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -36)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |SELECT
         |  key,
         |  cv,
         |  cvr,
         |  ctr,
         |  cvr_factor,
         |  jfb_factor,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_bs_params_pb_hourly_exp
         |WHERE
         |  $selectCondition
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .withColumn("create_time", concat_ws(" ", col("date"), col("hour")))
      .withColumn("time_stamp", unix_timestamp(col("create_time"), "yyyy-MM-dd HH"))
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  *,
         |  row_number() over(partition by unitid, conversion_goal, is_hidden, exp_tag order by time_stamp desc) as seq
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val dataRaw = spark.sql(sqlRequest2)

    val result = dataRaw
      .filter(s"seq = 1")
      .cache()
    result.show(10)

    result
  }

  def savePbPack(data: DataFrame, fileName: String, spark: SparkSession): Unit = {
    /*
    proto:
    message singleitem {
      string   key = 1;
      double   cvrcalfactor = 2;
      double   jfbfactor = 3;
      double   smoothfactor = 4;
      double   postcvr = 5;
      double   postctr = 6;
    }

    dataframe:
    ("key", "cv", "cvr", "ctr")
     */

    var list = new ListBuffer[SingleItem]
    var cnt = 0

    for (record <- data.collect()) {
      val key = record.getAs[String]("key")
      val postcvr = record.getAs[Double]("cvr")
      val postctr = record.getAs[Double]("ctr")
      val cvrFactor = record.getAs[Double]("cvr_factor")
      val jfbFactor = record.getAs[Double]("jfb_factor")

      if (cnt % 100 == 0) {
        println(s"key:$key, postcvr:$postcvr, postctr:$postctr, cvrFactor:$cvrFactor, jfbFactor:$jfbFactor")
      }
      cnt += 1

      val currentItem = SingleItem(
        key = key,
        cvrCalFactor = cvrFactor,
        jfbFactor = jfbFactor,
        smoothFactor = 0.0,
        postCvr = postcvr,
        postCtr = postctr
      )
      list += currentItem

    }
    val result = list.toArray[SingleItem]
    val adRecordList = OcpcBsParmasList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(fileName))

    println("complete save data into protobuffer")

  }


}
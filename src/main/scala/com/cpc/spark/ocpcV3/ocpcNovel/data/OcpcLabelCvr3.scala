package com.cpc.spark.ocpcV3.ocpcNovel.data

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcLabelCvr3 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 二类电商表单类转化
    val date = args(0).toString
    val hour = args(1).toString

    val result1 = getLabelFromAdv(date, hour, spark)
    result1
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpcv3_cvr2_data_hourly")
    println("successfully save data into table: dl_cpc.ocpcv3_cvr2_data_hourly")

  }

  def getLabelFromAdv(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    unitid,
         |    price,
         |    bid,
         |    userid,
         |    media_appsid,
         |    adclass,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_basedata_union_events
         |where day='$date' and hour = '$hour'
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002", "80002819")
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and is_api_callback!=1
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")


    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, 2)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  where $selectWhere
         |AND
         |  label = 1
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct()

    val resultDF = rawData
      .join(labelData, Seq("searchid"), "left_outer")
      .groupBy("ideaid", "unitid", "adclass", "media_appsid")
      .agg(sum(col("label")).alias("cvr2_cnt"))
      .select("ideaid", "unitid", "adclass", "media_appsid", "cvr2_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}

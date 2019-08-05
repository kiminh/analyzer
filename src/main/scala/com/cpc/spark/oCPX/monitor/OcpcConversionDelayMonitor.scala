package com.cpc.spark.oCPX.monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcConversionDelayMonitor {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt

    // 抽取基础表
    val baseData = getData(date, hour, hourInt, spark)

    // 分小时计算cvr
    val data = calculateCvr(baseData, date, hour, spark).cache()

    data.show(10)
    data
      .select("unitid", "userid", "conversion_goal", "media", "click", "cv1", "cv2")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_cvr_delay_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cvr_delay_hourly")

    // 计算丢失率
    val lostRate = calculateLostCvr(data, spark)
    lostRate
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_total_delay_cvr_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_total_delay_cvr_hourly")
  }

  def calculateLostCvr(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  conversion_goal,
         |  media,
         |  sum(click) as click,
         |  sum(cv1) as cv1,
         |  sum(cv2) as cv2,
         |  sum(cv1) * 1.0 / sum(cv2) as lost_cvr
         |FROM
         |  base_data
         |GROUP BY conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def calculateCvr(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then iscvr1 else 0 end) as cv1,
         |  sum(case when isclick=1 then iscvr2 else 0 end) as cv2
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def udfCalculateHourInt(date: String, hour: String) = udf((dateCol: String, hourCol: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val time1 = dateConverter.parse(date + " " + hour)
    val time2 = dateConverter.parse(dateCol + " " + hourCol)
    val timeDiff = time1.getTime() - time2.getTime()
    val diffHours = timeDiff / (60 * 60 * 1000);
    diffHours

  })

  def getData(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    media_appsid,
         |    isclick
         |from dl_cpc.ocpc_base_unionlog
         |where `date` = '$date1'
         |and `hour` = '$hour1'
         |and isshow = 1
         |and $mediaSelection
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))


    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr1,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` = '$date1'
         |and `hour` = '$hour1'
       """.stripMargin
    println(sqlRequest2)
    val cvData1 = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val cvData2 = spark.sql(sqlRequest3)


    // 关联数据
    val resultDF = clickData
      .join(cvData1, Seq("searchid", "cvr_goal"), "left_outer")
      .join(cvData2, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2"))

    resultDF
  }


}




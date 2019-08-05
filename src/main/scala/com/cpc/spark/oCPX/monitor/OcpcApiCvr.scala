package com.cpc.spark.oCPX.monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.github.jurajburian.mailer._
import com.typesafe.config.ConfigFactory
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcApiCvr {
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
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_api_cvr_data20190805")

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
         |  date,
         |  hour,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then label else 0 end) as click_cv,
         |  sum(label) as show_cv
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal, media, date, hour
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("hourInt", udfCalculateHourInt(date, hour)(col("date"), col("hour")))

    data
  }

  def udfCalculateHourInt(date: String, hour: String) = udf((dateCol: String, hourCol: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val time1 = dateConverter.parse(date + " " + hour)
    val time2 = dateConverter.parse(dateCol + " " + hourCol)
    val timeDiff = time2.compareTo(time1)
    timeDiff

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
         |    isclick,
         |    date,
         |    hour
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition
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
         |  label,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("label"))

    resultDF
  }


}




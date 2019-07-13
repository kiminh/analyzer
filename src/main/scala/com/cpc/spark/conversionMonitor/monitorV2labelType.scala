package com.cpc.spark.conversionMonitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.github.jurajburian.mailer._
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object monitorV2labelType {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val minCvDiff = args(2).toDouble
    val minCv = args(3).toInt

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)

    val todayRaw = getData(date, hour, spark)
    val yesterdayRaw = getData(date1, hour1, spark)

    val todayData = todayRaw
      .withColumn("cv_today", col("cv"))
      .select("label_type", "adclass", "cv_today")

    val yesterdayData = yesterdayRaw
      .withColumn("cv_yesterday", col("cv"))
      .select("label_type", "adclass", "cv_yesterday")

    val data = todayData
      .join(yesterdayData, Seq("label_type", "adclass"), "outer")
      .na.fill(0, Seq("cv_today", "cv_yesterday"))
      .select("label_type", "adclass", "cv_today", "cv_yesterday")
      .withColumn("is_warn", udfIsWarn(minCv, minCvDiff)(col("cv_today"), col("cv_yesterday")))
      .cache()

    data.show(10)

    data
      .select("label_type", "adclass", "cv_today", "cv_yesterday", "is_warn")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.conversion_monitor_for_v2")

  }


  def udfIsWarn(minCv: Int, minCvDiff: Double) = udf((cvToday: Int, cvYesterday: Int) => {
    var result = 0
    val cvDiff = Math.abs((cvToday.toDouble - cvYesterday.toDouble) / cvYesterday.toDouble)
    val cvSum = cvToday + cvYesterday
    if (cvDiff > minCvDiff && cvSum > minCv) {
      result = 1
    }

    result
  })

  def getData(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  adclass,
         |  label_type,
         |  count(distinct searchid) as cv
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  `date` = '$date'
         |AND
         |  label2 = 1
         |AND
         |  label_type in (1, 2, 3, 4, 5, 6)
         |GROUP BY adclass, label_type
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }


  def sendMail(txt: String, sub: String, to: Seq[String]): Boolean = {
    //val conf = ConfigFactory.load()
    val session = (SmtpAddress("smtp.exmail.qq.com", 25) :: SessionFactory())
      .session(Some("cpc_notify@aiclk.com" -> "nXBYjc8XVB6hCPv5"))
    val toAdd = to.map(new InternetAddress(_))
    val msg = Message(
      from = new InternetAddress("cpc_notify@aiclk.com"),
      subject = sub,
      content = Content().text(txt),
      to = toAdd)
    try {
      Mailer(session).send(msg)
      true
    } catch {
      case e: Exception =>
        println(e.getMessage)
        false
    }
  }


}




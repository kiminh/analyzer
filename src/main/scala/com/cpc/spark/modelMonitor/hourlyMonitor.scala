package com.cpc.spark.modelMonitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.github.jurajburian.mailer._
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.process._

object hourlyMonitor {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val modelName = args(2).toString
    val negativeDiff = args(3).toDouble
    val positiveDiff = args(4).toDouble
    val ratioDiff = args(5).toDouble
    val adslotTypeNum = args(6).toInt

    // 清理ok文件
    s"hadoop fs -rm hdfs://emr-cluster/user/cpc/wangjun/okdir/model_check/$modelName-$date-$hour.ok" !

    val dataToday = getData(date, hour, modelName, spark)
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val dataYesterday = getData(date1, hour, modelName, spark)

    // 数据对比
    val cmpResult = cmpData(dataToday, dataYesterday, spark)
    val result = cmpResult
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("model_name", lit(modelName))
      .select("adslot_type", "negative_yesterday", "negative_today", "positive_yesterday", "positive_today", "negative_diff", "positive_diff", "ratio_diff", "date", "hour", "model_name")

    result
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.model_sample_cmp_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.model_sample_cmp_hourly")

    // 数据监控
    val filterResult = result.filter(s"negative_diff > $negativeDiff or positive_diff > $positiveDiff or ratio_diff > $ratioDiff")
    val cnt = filterResult.count()
    val totalCnt = result.count()
    filterResult.show(10)

    // email content
    val message = s"training set for $modelName-$date-$hour is incorrect!"
    val sub = "model training dataset monitor is warning"
    var receiver = Seq[String]()
    receiver:+="wangjun02@qutoutiao.net"
    receiver:+="yanglei@qutoutiao.net"
    receiver:+="admodel@qutoutiao.net"
    receiver:+="wanlunjun@qutoutiao.net"
    if (cnt > 0 || totalCnt != adslotTypeNum) {
      sendMail(message, sub, receiver)
    } else {
      //输出标记文件
      s"hadoop fs -touchz hdfs://emr-cluster/user/cpc/wangjun/okdir/model_check/$modelName-$date-$hour.ok" !
    }

  }

  def cmpData(dataToday: DataFrame, dataYesterday: DataFrame, spark: SparkSession) = {
    val data0 = dataToday
      .withColumn("negative_today", col("negative_num"))
      .withColumn("positive_today", col("positive_num"))
      .withColumn("ratio_today", col("ratio"))
      .select("adslot_type", "negative_today", "positive_today", "ratio_today")

    val data1 = dataYesterday
      .withColumn("negative_yesterday", col("negative_num"))
      .withColumn("positive_yesterday", col("positive_num"))
      .withColumn("ratio_yesterday", col("ratio"))
      .select("adslot_type", "negative_yesterday", "positive_yesterday", "ratio_yesterday")

    val data = data1
      .join(data0, Seq("adslot_type"), "inner")
      .withColumn("negative_diff", (col("negative_today") - col("negative_yesterday")) / col("negative_yesterday"))
      .withColumn("positive_diff", (col("positive_today") - col("positive_yesterday")) / col("positive_yesterday"))
      .withColumn("ratio_diff", (col("ratio_today") - col("ratio_yesterday")) / col("ratio_yesterday"))
      .select("adslot_type", "negative_yesterday", "negative_today", "positive_yesterday", "positive_today", "negative_diff", "positive_diff", "ratio_diff")


    data
  }

  def getData(date: String, hour: String, modelName: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  negative_num,
         |  positive_num,
         |  positive_num * 1.0 / (positive_num + negative_num) as ratio,
         |  adslot_type
         |FROM
         |  dl_cpc.model_training_data_daily
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  model_name = '$modelName'
         |AND
         |  negative_num > 0
         |AND
         |  positive_num > 0
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
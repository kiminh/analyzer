package com.cpc.spark.conversionMonitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt}
import com.github.jurajburian.mailer._
import com.typesafe.config.ConfigFactory
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object monitorApiCvr {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val chargeThreshold = args(1).toDouble
    val cvrDiffThreshold = args(2).toDouble

    // 抽取基础表
    val baseData = getData(date, spark)

    // 统计相关指标
    // charge, click, cv
    val currentData = calculateData(baseData, spark)
    currentData
      .withColumn("date", lit(date))
      .withColumn("conversion_goal", lit(2))
      .repartition(10)
      .write.mode("overwrite").insertInto("test.cvr_monitor_daily")

    // 统计抽取前一天的数据
    // charge, click, cv
    val prevData = getPrevData(date, spark)


    // 计算两天平均日耗
    // 计算两天的cvr差别
    val warningData = getWarns(currentData, prevData, chargeThreshold, cvrDiffThreshold, spark)
    warningData
      .withColumn("date", lit(date))
      .withColumn("conversion_goal", lit(2))
      .repartition(10)
      .write.mode("overwrite").insertInto("test.conversion_monitor_warning_users_daily")


  }

  def getWarns(currentData: DataFrame, prevData: DataFrame, minCharge: Double, minCvrDiff: Double, spark: SparkSession) = {
    val current = currentData
      .withColumn("click0", col("click"))
      .withColumn("cv0", col("cv"))
      .withColumn("charge0", col("charge"))
      .select("userid", "media", "click0", "cv0", "charge0")
    val prev = prevData
      .withColumn("click1", col("click"))
      .withColumn("cv1", col("cv"))
      .withColumn("charge1", col("charge"))
      .select("userid", "media", "click1", "cv1", "charge1")

    val data = current
      .join(prev, Seq("userid", "media"), "inner")
      .select("userid", "media", "click0", "cv0", "charge0", "click1", "cv1", "charge1")
      .withColumn("charge", col("charge1") + col("charge0"))
      .withColumn("cvr0", col("cv0") * 1.0 / col("click0"))
      .withColumn("cvr1", col("cv1") * 1.0 / col("click1"))
      .withColumn("cvr_diff", abs(col("cvr0") - col("cvr1")) * 1.0 / col("cvr1"))
      .withColumn("is_warn", udfCheckIsWarn(minCharge, minCvrDiff)(col("charge"), col("cvr_diff")))

    data
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_conversion_monitor20190712a")

    val result = data
      .filter(s"is_warn = 1")
      .withColumn("current_charge", col("charge0"))
      .withColumn("prev_charge", col("charge1"))
      .withColumn("current_cv", col("cv0"))
      .withColumn("prev_cv", col("cv1"))
      .withColumn("current_click", col("click0"))
      .withColumn("prev_click", col("click1"))
      .select("userid", "media", "current_click", "prev_click", "current_cv", "prev_cv", "current_charge", "prev_charge")
      .cache()

    result.show(10)
    result
  }


  def udfCheckIsWarn(minCharge: Double, minCvrDiff: Double) = udf((charge: Double, cvrDiff: Double) => {
    var result = 0
    if (charge >= minCharge && cvrDiff >= minCvrDiff) {
      result = 1
    }
    result
  })

  def getPrevData(date: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` = '$date1'"

    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  media,
         |  click,
         |  cv,
         |  charge
         |FROM
         |  test.cvr_monitor_daily
         |WHERE
         |  $selectCondition
         |AND
         |  conversion_goal = 2
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

  def calculateData(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  media,
         |  sum(isclick) as click,
         |  sum(label) as cv,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as charge
         |FROM
         |  base_data
         |GROUP BY userid, media
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

  def getData(date: String, spark: SparkSession) = {
    // 取历史数据
    val selectCondition = s"`date` = '$date'"

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    userid,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    isclick,
         |    price
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition
         |and isclick = 1
         |and $mediaSelection
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |and is_api_callback = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark.sql(sqlRequest)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = clickData
      .join(cvData, Seq("searchid"), "left_outer")
      .select("searchid", "userid", "media", "isclick", "price", "label")
      .na.fill(0, Seq("label"))
      .select("searchid", "userid", "media", "isclick", "price", "label")

    resultDF
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




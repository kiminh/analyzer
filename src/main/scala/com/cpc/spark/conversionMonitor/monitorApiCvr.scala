package com.cpc.spark.conversionMonitor

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt}
import com.github.jurajburian.mailer._
import com.typesafe.config.ConfigFactory
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object monitorApiCvr {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

//    val cnt3 = getDataV3(date, hour, spark)
//    val cnt4 = getDataV4(date, hour, spark)
//    val cnt5 = getDataV5(date, hour, spark)
//    println(s"v3 = $cnt3, v4 = $cnt4, v5 = $cnt5")
//
//    var message = ""
//
//    val cntDiffPercent = (cnt3.toDouble - cnt4.toDouble) / cnt3.toDouble
//    if (cntDiffPercent < 0 || cntDiffPercent > 0.15) {
//      message = message + "v4 abnormal\n"
//    }
//    if (cnt3 != cnt5) {
//      message = message + "v5 abnormal\n"
//    }
//    val sub = "api conversion monitor warning!"
//    var receiver = Seq[String]()
//    receiver:+="wangjun02@qutoutiao.net"
////    receiver:+="hanzhengding@qutoutiao.net"
////    receiver:+="zhanghongyang@qutoutiao.net"
////    receiver:+="wangyao@qutoutiao.net"
////    receiver:+="dongjinbao@qutoutiao.net"
////    receiver:+="chuquanquan@qutoutiao.net"
//    println(message)
//    if (message != "") {
//      message += s"date=$date, hour=$hour: v3 = $cnt3, v4 = $cnt4, v5 = $cnt5"
//      sendMail(message, sub, receiver)
//    }


  }

  def getData(hourInt: Int, version: String, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition1 = getTimeRangeSqlDate(date1, hour1, date, hour)

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    cast(unitid as string) identifier,
         |    cast(exp_cvr * 1000000 as bigint) as score,
         |    conversion_goal,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition1
         |and isclick = 1
         |and $mediaSelection
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |and is_ocpc = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    // 取历史区间: cvr数据
    val selectCondition2 = s"`date`>='$date1'"
    // 抽取数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid", "cvr_goal"), "left_outer")
      .select("searchid", "identifier", "media", "conversion_goal", "score", "label", "industry")
      .na.fill(0, Seq("label"))
      .select("searchid", "identifier", "media", "conversion_goal", "score", "label", "industry")
      .withColumn("version", lit(version))

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




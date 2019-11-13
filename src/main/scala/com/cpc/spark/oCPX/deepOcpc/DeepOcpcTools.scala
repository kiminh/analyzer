package com.cpc.spark.oCPX.deepOcpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DeepOcpcTools {
  def main(args: Array[String]): Unit = {
    /*
    代码测试
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val hour = args(1).toString

    // 测试两个抽取数据的程序
    val data1 = getDeepData(23, date, hour, spark)
    val data2 = getDeepDataDelay(23, date, hour, spark)

    data1
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20191008a")

    data2
      .write.mode("overwrite").saveAsTable("test.check_deep_ocpc_data20191008b")

  }

  def getTimeRangeSqlDay(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`day` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`day` = '$startDate' and hour > '$startHour') " +
      s"or (`day` = '$endDate' and hour <= '$endHour') " +
      s"or (`day` > '$startDate' and `day` < '$endDate'))"
  }

  def getTimeRangeSqlDate(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def udfConcatStringInt(str: String) = udf((intValue: Int) => {
    val result = str + intValue.toString
    result
  })

  def getDeepData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

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

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  deep_cvr * 1.0 / 1000000 as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  deep_conversion_goal as conversion_goal,
         |  expids,
         |  exptags,
         |  ocpc_expand,
         |  (case when hidden_tax is null then 0 else hidden_tax end) as hidden_tax,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
         |AND
         |  deep_cvr_model_name is not null
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  deep_cvr > 0
         |AND
         |  deep_cvr is not null
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  deep_conversion_goal as conversion_goal
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def getDeepDataDelay(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

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

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  deep_cvr * 1.0 / 1000000 as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  deep_conversion_goal as conversion_goal,
         |  expids,
         |  exptags,
         |  ocpc_expand,
         |  (case when hidden_tax is null then 0 else hidden_tax end) as hidden_tax,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
         |AND
         |  deep_cvr_model_name is not null
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  deep_cvr > 0
         |AND
         |  deep_cvr is not null
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  deep_conversion_goal as conversion_goal
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def udfMediaName() = udf((media: String) => {
    val result = media match {
      case "qtt" => "Qtt"
      case "hottopic" => "HT66"
      case "novel" => "MiDu"
      case _ => "others"
    }
    result
  })

  def udfDetermineMedia() = udf((mediaId: String) => {
    val result = mediaId match {
      case "80000001" => "qtt"
      case "80000002" => "qtt"
      case "80002819" => "hottopic"
      case "80004944" => "hottopic"
      case "80004948" => "hottopic"
      case _ => "novel"
    }
    result
  })
}
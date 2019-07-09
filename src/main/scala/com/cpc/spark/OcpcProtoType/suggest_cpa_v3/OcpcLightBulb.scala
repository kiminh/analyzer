package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
//import com.cpc.spark.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulb{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
//    2019-02-02 10 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    // todo 修改表名
    val tableName = "dl_cpc.ocpc_light_control_version"
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")


    // 抽取数据
    val cpcData = getRecommendationAd(version, date, hour, spark)
    val ocpcData = getOcpcRecord(version, date, hour, spark)
    val confData = getConfCPA(version, date, hour, spark)


    val result = cpcData
        .join(ocpcData, Seq("unitid", "userid", "adclass", "media"), "outer")
        .join(confData, Seq("unitid", "media"), "outer")
        .select("unitid", "userid", "adclass", "media", "cpa1", "cpa2", "cpa3")
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa3"))
        .withColumn("cpa", udfSelectCPA()(col("cpa1"), col("cpa2"), col("cpa3")))
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa3", "cpa"))

    result.show(10)
    val resultDF = result
      .select("unitid", "userid", "adclass", "media", "cpa")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_unit_light_control_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_light_control_hourly")
//
//    resultDF
//      .select("unitid", "userid", "adclass", "media", "cpa", "version")
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_light_control_version")
  }

  def udfSelectCPA() = udf((cpa1: Double, cpa2: Double, cpa3: Double) => {
    var cpa = 0.0
    if (cpa3 >= 0) {
      cpa = cpa3
    } else if (cpa2 >= 0) {
      cpa = cpa2
    } else {
      cpa = cpa1
    }

    cpa
  })

  def getConfCPA(version: String, date: String, hour: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val suggestCpaPath = conf.getString("ocpc_all.light_control.suggest_path_v2")
    val rawData = spark.read.format("json").json(suggestCpaPath)
    val data = rawData
      .filter(s"version = '$version'")
      .groupBy("unitid", "media")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest")
      )
      .withColumn("cpa3", col("cpa_suggest") * 0.01)
      .selectExpr("unitid", "media", "cpa3")

    data
      .withColumn("cpa", col("cpa3"))
      .select("unitid", "media", "cpa")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_light_qtt_manual_list_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_qtt_manual_list_version")
    data
  }

  def getOcpcRecord(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取最近三天所有广告单元的投放记录
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSqlDate(date1, hour, date, hour)

    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    $mediaSelection
         |AND
         |    is_ocpc = 1
         |AND
         |    isclick = 1
       """.stripMargin
    println(sqlRequest1)
    val rawData1 = spark
      .sql(sqlRequest1)
      .filter(s"is_hidden = 0")
      .select("unitid", "userid", "adclass", "media")
      .distinct()

    // 取近三小时数据
    val hourConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val newToday = hourConverter.parse(newDate)
    val newCalendar = Calendar.getInstance
    newCalendar.setTime(newToday)
    newCalendar.add(Calendar.HOUR, +3)
    val newYesterday = newCalendar.getTime
    val prevTime = hourConverter.format(newYesterday)
    val prevTimeValue = prevTime.split(" ")
    val newDate1 = prevTimeValue(0)
    val newHour1 = prevTimeValue(1)
    val newSelectCondition = getTimeRangeSqlDay(date, hour, newDate1, newHour1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'qtt'
         |      when media_appsid in ('80002819') then 'hottopic'
         |      else 'novel'
         |  end) as media
         |FROM
         |  dl_cpc.cpc_basedata_click_event
         |WHERE
         |  $newSelectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
         |AND
         |  ocpc_step = 2
       """.stripMargin
    println(sqlRequest2)
    val rawData2 = spark
      .sql(sqlRequest2)
      .select("unitid", "userid", "adclass", "media")
      .distinct()
    val rawData = rawData1
      .join(rawData2, Seq("unitid", "userid", "adclass", "media"), "outer")
      .select("unitid", "userid", "adclass", "media")
      .distinct()

    val sqlRequet3 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  media,
         |  cpa_suggest * 1.0 / 100 as cpa_suggest
         |FROM
         |  dl_cpc.ocpc_history_suggest_cpa_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequet3)
    val suggestDataRaw1 = spark.sql(sqlRequet3)

    val sqlRequest4 =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  cpa * 1.0 / 100 as cpa_manual
         |FROM
         |  test.ocpc_light_qtt_manual_list_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequest4)
    val suggestDataRaw2 = spark.sql(sqlRequest4)

//    val suggestDataRaw = suggestDataRaw1
//      .join(suggestDataRaw2, Seq("unitid"), "outer")
//      .withColumn("cpa2", when(col("cpa_manual").isNotNull, col("cpa_manual")).otherwise(col("cpa_suggest")))
//      .na.fill(0, Seq("cpa2"))

    val result = rawData
      .join(suggestDataRaw1, Seq("unitid", "userid", "adclass", "media"), "left_outer")
      .select("unitid", "userid", "adclass", "media", "cpa_suggest")
      .join(suggestDataRaw2, Seq("unitid", "media"), "left_outer")
      .select("unitid", "userid", "adclass", "media", "cpa_suggest", "cpa_manual")
      .withColumn("cpa2", when(col("cpa_manual").isNotNull, col("cpa_manual")).otherwise(col("cpa_suggest")))
      .na.fill(0, Seq("cpa2"))

    result.show(10)
    val resultDF = result.select("unitid", "userid", "adclass", "media", "cpa2")

    resultDF
  }

  def getRecommendationAd(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    media,
         |    cpa * 1.0 / 100 as cpa1
         |FROM
         |    dl_cpc.ocpc_recommend_units_hourly
         |WHERE
         |    date = '$date'
         |AND
         |    `hour` = '$hour'
         |and is_recommend = 1
         |and version = '$version'
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }

}

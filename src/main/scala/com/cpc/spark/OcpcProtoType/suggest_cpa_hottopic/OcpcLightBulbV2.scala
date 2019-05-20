package com.cpc.spark.OcpcProtoType.suggest_cpa_hottopic

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcLightBulbV2._


object OcpcLightBulbV2{
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
    val media = args(3).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour, $version, $media")
      .enableHiveSupport().getOrCreate()

    // todo 修改表名
    val tableName = "dl_cpc.ocpc_light_control_version"
//    val tableName = "test.ocpc_qtt_light_control_v2"
//    val tableName = "test.ocpc_qtt_light_control_version20190415"
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")


    // 抽取数据
    val cpcData = getRecommendationAdV2(version, date, hour, spark)
//    cpcData.write.mode("overwrite").saveAsTable("test.check_ocpc_light_control_20190511a")
    val ocpcData = getOcpcRecordV2(media, version, date, hour, spark)
//    ocpcData.write.mode("overwrite").saveAsTable("test.check_ocpc_light_control_20190511b")
    val confData = getConfCPA(media, date, hour, spark)
    val cvUnit = getCPAgiven(date, hour, spark)


    val data = cpcData
        .join(ocpcData, Seq("unitid", "conversion_goal"), "outer")
        .join(confData, Seq("unitid", "conversion_goal"), "outer")
        .select("unitid", "conversion_goal", "cpa1", "cpa2", "cpa3")
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa3"))
        .withColumn("cpa", udfSelectCPA()(col("cpa1"), col("cpa2"), col("cpa3")))
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa3", "cpa"))
//    data.write.mode("overwrite").saveAsTable("test.check_ocpc_light_control_20190511c")

    data.show(10)

    val resultDF = data
        .join(cvUnit, Seq("unitid", "conversion_goal"), "inner")
        .select("unitid", "conversion_goal", "cpa")
        .selectExpr("cast(unitid as string) unitid", "conversion_goal", "cpa")
        .withColumn("date", lit(date))
        .withColumn("version", lit(version))

    resultDF.show(10)

    resultDF
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_light_control_daily")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_daily")

    // 清除redis里面的数据
    println(s"############## cleaning redis database ##########################")
    cleanRedis(tableName, version, date, hour, spark)

    // 存入redis
    saveDataToRedis(version, date, hour, spark)
    println(s"############## saving redis database ##########################")

    resultDF
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_qtt_light_control_version20190415")
      .repartition(5).write.mode("overwrite").insertInto(tableName)


  }


  def getRecommendationAdV2(version: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    conversion_goal,
         |    cpa * 1.0 / 100 as cpa1
         |FROM
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |    date = '$date'
         |AND
         |    `hour` = '$hour'
         |and is_recommend = 1
         |and version = '$version'
         |and industry in ('feedapp')
         |and conversion_goal = 1
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .distinct()

    resultDF.show(10)
    resultDF
  }

  def getOcpcRecordV2(media: String, version: String, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry
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
    val rawData = spark
      .sql(sqlRequest1)
      .filter(s"is_hidden = 0")
      .filter(s"industry in ('feedapp')")
      .distinct()

    val sqlRequets2 =
      s"""
         |SELECT
         |  cast(identifier as int) as unitid,
         |  conversion_goal,
         |  cpa_suggest * 1.0 / 100 as cpa2
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_k_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequets2)
    val suggestDataRaw = spark.sql(sqlRequets2)

    val result = rawData
      .join(suggestDataRaw, Seq("unitid", "conversion_goal"), "left_outer")
      .select("unitid", "conversion_goal", "cpa2")
      .na.fill(0, Seq("cpa2"))

    result.show(10)
    val resultDF = result.select("unitid", "conversion_goal", "cpa2")

    resultDF
  }

}

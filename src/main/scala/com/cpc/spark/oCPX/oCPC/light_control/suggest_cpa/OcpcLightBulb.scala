package com.cpc.spark.oCPX.oCPC.light_control.suggest_cpa

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.redis.RedisClient
import org.apache.spark.sql.DataFrame
//import com.cpc.spark.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulb{
  def main(args: Array[String]): Unit = {
    /*
    控制打开灯泡
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulb: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 抽取推荐cpa数据和白名单单元
    val suggestUnits = getRecommendationAd(version, date, hour, spark)

    // 检查线上的媒体id进行校验
    val units = getUnitData(spark)

    // 推送进入准入表
    val resultDF = assemblyData(suggestUnits, units, spark)

    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_light_api_control_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_hourly")

  }

  def assemblyData(suggestUnits: DataFrame, unitsRaw: DataFrame, session: SparkSession) = {
    val units = unitsRaw.select("unitid", "media").distinct()
    val data = suggestUnits
      .join(units, Seq("unitid", "media"), "inner")
      .withColumn("unit_id", col("unitid"))
      .withColumn("ocpc_light", lit(1))
      .selectExpr("unit_id", "ocpc_light", "cast(round(current_cpa, 2) as double) as ocpc_suggest_price")

    data
  }

  def getUnitData(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, cast(conversion_goal as char) as conversion_goal, target_medias, is_ocpc, ocpc_status, create_time from adv.unit where ideas is not null and is_ocpc = 1 and ocpc_status != 2) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val rawData = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .selectExpr("unitid",  "userid", "cast(conversion_goal as int) conversion_goal", "cast(is_ocpc as int) is_ocpc", "ocpc_status", "target_medias", "create_time")
      .distinct()

    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    is_ocpc,
         |    ocpc_status,
         |    target_medias,
         |    cast(a as string) as media_appsid
         |from
         |    raw_data
         |lateral view explode(split(target_medias, ',')) b as a
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
      .na.fill("", Seq("media_appsid"))
      .withColumn("media", udfDetermineMediaNew()(col("media_appsid")))
      .filter(s"media in ('qtt', 'hottopic', 'novel', 'others')")
      .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media")
      .distinct()


    resultDF.show(10)
    resultDF
  }

  def udfDetermineMediaNew() = udf((mediaId: String) => {
    var result = mediaId match {
      case "80000001" => "qtt"
      case "80000002" => "qtt"
      case "80002819" => "hottopic"
      case "80004944" => "hottopic"
      case "80004948" => "hottopic"
      case "80004953" => "hottopic"
      case "" => "qtt"
      case "80001098" => "novel"
      case "80001292" => "novel"
      case "80001539" => "novel"
      case "80002480" => "novel"
      case "80001011" => "novel"
      case "80004786" => "novel"
      case "80004787" => "novel"
      case _ => "others"
    }
    result
  })

  def getRecommendationAd(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    media,
         |    min(cpa) * 1.0 / 100 as cpa1
         |FROM
         |    dl_cpc.ocpc_recommend_units_hourly
         |WHERE
         |    date = '$date'
         |AND
         |    `hour` = '$hour'
         |and version = '$version'
         |and is_recommend = 1
         |and ocpc_status != 2
         |group by unitid, media
       """.stripMargin

    println(sqlRequest)
    val data1 = spark.sql(sqlRequest)

    val data2raw = getConfCPA(version, date, hour, spark)
    val data2 = data2raw
      .withColumn("cpa2", col("cpa_suggest") * 1.0 / 100)
      .select("unitid", "media", "cpa2")

    val result = data1
      .join(data2, Seq("unitid", "media"), "outer")
      .select("unitid", "media", "cpa1", "cpa2")
      .withColumn("cpa", when(col("cpa2").isNotNull, col("cpa2")).otherwise(col("cpa1")))

    val resultDF = result
      .select("unitid", "media", "cpa")


    resultDF.show(10)
    resultDF
  }

}

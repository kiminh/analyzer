package com.cpc.spark.oCPX.oCPC.light_control.white_list

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcWhiteList {
  def main(args: Array[String]): Unit = {
    /*
    从adv实时读取unit表和user表，根据进行单元拼接，并根据category，找到白名单行业，调整投放白名单
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString

    // 获取广告主
    // userid, category
    val user = getUserData(spark)
    user.printSchema()

    // 获取广告单元
    val unit = getUnitData(spark)
    unit.printSchema()

    // 获取ocpc行业白名单
    val conf = ConfigFactory.load("ocpc")
    val adclassList = conf.getIntList(s"ocpc_light_white_list.$version.adclass")
    val adclassStringList = adclassList
        .toString
        .replace("[", "")
        .replace("]", "")
//    println(adclassList.get(0))
    println(adclassStringList)
    adclassList

    // 数据关联
    val adclassSelection = "adclass in (" + adclassStringList + ")"
    println(adclassSelection)

    val filterUser = user.filter(adclassSelection).distinct()
    val filterUnit = unit
      .join(filterUser, Seq("userid"), "inner")

//    filterUser
//      .repartition(1)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_white_units20191018a")
//
//    filterUnit
//      .repartition(1)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_white_units20191018b")
    filterUnit
      .select("unitid", "userid", "conversion_goal", "adclass", "ocpc_status", "media")
      .withColumn("ocpc_light", lit(1))
      .withColumn("ocpc_suggest_price", lit(0.0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_light_control_white_units_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_white_units_hourly")
//
  }

  def getUserData(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, category from adv.user where category > 0) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("userid", col("id"))
      .withColumn("adclass", col("category"))
      .selectExpr("userid", "adclass")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getUnitData(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, cast(conversion_goal as char) as conversion_goal, target_medias, is_ocpc, ocpc_status from adv.unit where ideas is not null and is_ocpc = 1 and ocpc_status not in (2, 4)) as tmp"

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
      .selectExpr("unitid",  "userid", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status", "target_medias")
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
    val resultDF = spark
      .sql(sqlRequest)
      .na.fill("", Seq("media_appsid"))
      .withColumn("media", udfDetermineMediaNew()(col("media_appsid")))
      .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media")
      .filter(s"media in ('qtt', 'hottopic')")
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
      case "" => "qtt"
      case _ => "novel"
    }
    result
  })



}
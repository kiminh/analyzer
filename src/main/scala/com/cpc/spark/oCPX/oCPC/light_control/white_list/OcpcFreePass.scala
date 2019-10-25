package com.cpc.spark.oCPX.oCPC.light_control.white_list

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.oCPX.OcpcTools._


object OcpcFreePass {
  def main(args: Array[String]): Unit = {
    /*
    oCPC零门槛实验
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

    // 获取同账户是否有历史数据
    val historyData = getHistoryData(date, spark)

    // 数据关联
    val joinData = unit
        .join(user, Seq("userid"), "inner")
        .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "adclass", "industry")
        .join(historyData, Seq("userid", "conversion_goal", "media"), "left_outer")
        .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "adclass", "industry", "cost_flag")
        .withColumn("flag_ratio", udfCalculateRatio()(col("conversion_goal"), col("industry"), col("media"), col("cost_flag")))
        .withColumn("random_value", udfGetRandom()())
        .withColumn("flag", when(col("flag") < col("flag_ratio"), 1).otherwise(0))


    user
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_white_units20191025a")

    unit
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_white_units20191025b")

    joinData
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_white_units20191025c")


    //    joinData
//      .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "adclass", "industry", "cost_flag")
//      .withColumn("ocpc_light", lit(1))
//      .withColumn("ocpc_suggest_price", lit(0.0))
//      .select("unitid", "userid", "conversion_goal", "adclass", "ocpc_status", "ocpc_light", "ocpc_suggest_price", "media")
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//      .repartition(1)
////      .write.mode("overwrite").insertInto("test.ocpc_light_control_white_units_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_white_units_hourly")
////
  }

  def udfGetRandom() = udf(() => {
    val r = scala.util.Random
    val result = r.nextInt(100)
    result
  })

  def udfCalculateRatio() = udf((conversionGoal: Int, industry: String, media: String, costFlag: Int) => {
    var result = (conversionGoal, industry, media, costFlag) match {
      case (1, "app", "hottopic", _) => 50
      case (2, "app", "qtt", _) => 50
      case (2, "app", "novel", _) => 50
      case (_, "yihu", "qtt", 1) => 50
      case (_, _, _, _) => 0
    }
  })

  def getHistoryData(date: String, spark: SparkSession) = {
    /*
    从报表数据集中抽取历史消费数据，检查每个userid在不同转化目标和媒体下最近10天是否有ocpc二阶段消费
    dl_cpc.ocpc_report_base_hourly
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -11)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` >= '$date1' and `date` < '$date'"

    // 抽取数据
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  conversion_goal,
         |  media,
         |  1 as cost_flag
         |FROM
         |  dl_cpc.ocpc_report_base_hourly
         |WHERE
         |  $selectCondition
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).distinct()
    data
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
      .selectExpr("userid", "cast(adclass as int) as adclass")
      .withColumn("industry", udfDetermineIndustryV2()(col("adclass")))
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
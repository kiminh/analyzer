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

    // 获取广告主
    // userid, category
    val user = getUserData(spark)
    user.printSchema()

    // 获取广告单元
    val unit = getUnitData(spark)
    unit.printSchema()

    // 获取ocpc行业白名单
    val conf = ConfigFactory.load("ocpc")
    val adclassList = conf.getIntList("ocpc_light_white_list.v1.adclass")
    val adclassStringList = adclassList.toString
    println(adclassList.get(0))
    println(adclassStringList)
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
    val table = "(select id, user_id, cast(conversion_goal as char) as conversion_goal, is_ocpc, ocpc_status from adv.unit where ideas is not null and is_ocpc = 1 and ocpc_status not in (2, 4)) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .selectExpr("unitid",  "userid", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status")
      .distinct()

    resultDF.show(10)
    resultDF
  }


}
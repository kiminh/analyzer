package com.cpc.spark.OcpcProtoType

import java.io.{FileInputStream, FileOutputStream, InputStreamReader, ObjectInputStream}
import java.nio.file.FileSystem

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem}

object OcpcTools {
  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, ocpc_bid, cast(conversion_goal as char) as conversion_goal, is_ocpc, ocpc_status from adv.unit where ideas is not null) as tmp"

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
      .withColumn("cpagiven", col("ocpc_bid"))
      .selectExpr("unitid",  "userid", "cpagiven", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status")
      .distinct()

    resultDF.show(10)
    resultDF
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
      .selectExpr("unitid", "media", "cpa_suggest")

    data.show()
    data
  }

  def getExpConf(version: String, expTag: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val tag = "ocpc_exp." + version + "." + expTag
    val conf = ConfigFactory.load(tag)
    conf
  }

  def udfDetermineMedia() = udf((mediaId: String) => {
    var result = mediaId match {
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
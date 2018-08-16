package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import userprofile.Userprofile.{InterestItem, UserProfile}
import scala.util.control._
import com.cpc.spark.qukan.userprofile.SetUserProfileTag


/***
  * Create by myt on 19/07/2018
  *
  */

object TagFromHivetoRedis {
  def main(args: Array[String]): Unit = {
    val op = args(0).toInt
    val date = args(1).toString
    val hour = {if (op == 0) {""} else {args(2).toString}}


    val spark = SparkSession.builder()
      .appName("TagFromHivetoRedis")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    var tagList = spark.sql(
      if (op == 0) {
        "show partitions  dl_cpc.cpc_userprofile_tag_daily partition(`date` = \"%s\")".format(date)
      } else if (op == 1){
        "show partitions  dl_cpc.cpc_userprofile_tag_hourly partition(`date` = \"%s\", `hour` = \"%s\")".format(date, hour)
      } else {
        ""
      }
    ).rdd.map {
      x =>x.getAs[String](0).slice(-3, -1)
    }
    tagList.toLocalIterator.foreach(println)
    /*
    for (i <- tagList ) {
      var stmt = ""
      if (op == 0) {
        stmt =
          """
            |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_daily where `date` = "%s" and tag = %s
          """.stripMargin.format(date, i)
      } else if (op == 1){
        stmt =
          """
            |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and `hour` = "%s" and tag = %s
          """.stripMargin.format(date, hour, i)
      }
      if (stmt != "") {
        println(stmt)
        val rs = spark.sql(stmt).rdd.map{
          r =>
            (r.getAs[String](0), r.getAs[Int](1), r.getAs[Boolean](2))
        }
        SetUserProfileTag.setUserProfileTag(rs)
      }
    }
    */
  }
}

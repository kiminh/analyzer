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
    val tagList = Array[Int](201, 202, 203, 204, 205, 206, 207, 208, 209, 212, 216, 218, 219, 220, 221, 222,
      223, 224, 225, 226, 227, 228, 230, 231, 233, 234, 235, 236, 237, 238, 239, 240, 241)
    val op = args(0).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -2)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)

    val spark = SparkSession.builder()
      .appName("TagFromHivetoRedis")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    for (i <- tagList ) {
      var stmt = ""
      if (op == 0) {
        stmt =
          """
            |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and tag = %s"
          """.stripMargin.format(date, i)
      } else if (op == 1){
        stmt =
          """
            |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and `hour` = "%s" and tag = %s"
          """.stripMargin.format(date, hour, i)
      }
      if (stmt != "") {
        val rs = spark.sql(stmt).rdd.map{
          r =>
            (r.getAs[String](0), r.getAs[Int](1), r.getAs[Boolean](2))
        }
        SetUserProfileTag.setUserProfileTag(rs)
      }
    }
  }
}

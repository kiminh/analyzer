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

object TagUserByLx {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val is_set = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag user by lx")
      .enableHiveSupport()
      .getOrCreate()

    val lx_package =
      """
        |(
        |1258,
        |410,
        |1614,
        |305,
        |751,
        |429,
        |430,
        |1604,
        |407,
        |781,
        |716,
        |1816,
        |779
        |)
      """.stripMargin
    val cal = Calendar.getInstance()
    val
    import spark.implicits._

    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log
        |where `date` >= "%s" and `date` < "%s" and media_appsid  in ("80000001", "80000002") and ext_int["lx_package"] in %s
      """.stripMargin.format(lx_package)

    val lx = spark.sql(stmt).rdd.map {
      r =>
        val uid = r.getAs[String](0)
        (uid, 235, true)
    }
    println(lx.count())
    val ret = SetUserProfileTag.testSetUserProfileTag(lx)

  }

}

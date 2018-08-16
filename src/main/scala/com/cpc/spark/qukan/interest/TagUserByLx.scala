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
    import spark.implicits._

    val lx_package =
      """
        |(32,33,34,37,55,325,326,355,356,876,877,878,879,880,881,882,883,884,885,1333,1334,1335,1336,1337,1338,1607,1608,1609,1610,1612,1613,1614,1615,1616,1642,1643,1644,1645,1646,1647,1648,1649,1744,1745,1746,1747,1805,1806,1807,1808,1809,1810,1811,1812,1813,1814,1815,1816,1817,1818,1819,1820,1821,1822,1823,1824,1892,1893,1894,1897,1898,1900,1901,1902,1903,1914,1915,1917,2003,2103,2104,2105,2106,2107,2154,2155,2156,4000,4001,4002,4003,4004,4005,4006,4007,4008,4009,4010,4011)
      """.stripMargin
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val edate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log
        |where `date` >= "%s" and `date` < "%s" and media_appsid  in ("80000001", "80000002") and ext_int["lx_package"] in %s
      """.stripMargin.format(sdate, edate, lx_package)
    println(stmt)
    val lx = spark.sql(stmt).rdd.map {
      r =>
        val uid = r.getAs[String](0)
        (uid, 235, true)
    }
    println(lx.count())
    lx.take(10).foreach(println)
    val ret = SetUserProfileTag.SetUserProfileTagInHiveDaily(lx)

  }

}

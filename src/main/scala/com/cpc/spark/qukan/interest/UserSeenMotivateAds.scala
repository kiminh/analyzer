package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.userprofile.SetUserProfileTag
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  */
object UserSeenMotivateAds {

  val interactAdslotIds = Seq(
    7004278,
    7325125,
    7543281,
    7566682,
    7568984,
    7741271,
    7815672,
    7987042)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("user seen motivate ads")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = args(0)
    val stmt =
      """
        |select uid from dl_cpc.cpc_union_log where `date` = "%s" and adslotid in (%s)
      """.stripMargin.format(date, interactAdslotIds.mkString(","))

    println(stmt)
    val ipreg = """^[0-9.]+$""".r
    val uidRDD = spark.sql(stmt)
      .distinct()
      .filter(r => ipreg.findFirstMatchIn(r.getString(0)).isEmpty)

    println("num", uidRDD.count())
    println(uidRDD.take(10).mkString(" "))

    val sum = SetUserProfileTag.setUserProfileTag(uidRDD.rdd.map(x => (x.getString(0), 227, true)))
    sum.foreach(println)
  }
}


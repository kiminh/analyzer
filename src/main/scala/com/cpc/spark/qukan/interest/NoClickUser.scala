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
object NoClickUser {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val days = args(0)
    val spark = SparkSession.builder()
      .appName("no click user >= %s".format(days))
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)
    var date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var stmt =
      """
        |select uid, max(isclick) c from dl_cpc.cpc_union_log where `date` = "%s"
        |and media_appsid in ("80000001", "80000002") group by uid having c = 0
      """.stripMargin.format(date)
    println(stmt)
    val uidRDD1 = spark.sql(stmt)

    cal.add(Calendar.DATE, 1)
    date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    stmt =
      """
        |select uid, max(isclick) c from dl_cpc.cpc_union_log where `date` = "%s"
        |and media_appsid in ("80000001", "80000002") group by uid having c = 0
      """.stripMargin.format(date)
    println(stmt)
    val uidRDD2 = spark.sql(stmt)

    cal.add(Calendar.DATE, 1)
    date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    stmt =
      """
        |select uid, max(isclick) c from dl_cpc.cpc_union_log where `date` = "%s"
        |and media_appsid in ("80000001", "80000002") group by uid having c = 0
      """.stripMargin.format(date)
    println(stmt)
    val uidRDD3 = spark.sql(stmt)

    val uidRDD = uidRDD1.join(uidRDD2, Seq("uid")).join(uidRDD3, Seq("uid")).coalesce(200)

    println("num", uidRDD.count())
    println(uidRDD.take(10).mkString(" "))

    val sum = SetUserProfileTag.SetUserProfileTagInHiveDaily(uidRDD.rdd.map(x => (x.getString(0), 230, true)))
    sum.foreach(println)
  }
}


package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.qukan.userprofile.SetUserProfileTag
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  * Created by zhaolei on 15/1/2018.
  * 保健品人群包增加 点击保健品行业广告用户
  */
object InterestedCategoryBJP {

  val topCates = Seq(130104)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("cpc get user interested category")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    for (adclass <- topCates) {
      val cal = Calendar.getInstance()

      cal.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val day2 = spark.sql(
        """
          |select isclick,uid from dl_cpc.cpc_union_log where `date` = "%s"
          |and round(ext['adclass'].int_value / 1e3,0) = %d and isshow = 1
        """.stripMargin.format(date, adclass)).rdd
        .repartition(1000)
        .map {
          u =>
            (u.getAs[String]("uid"), (u.getAs[Int]("isclick"), 1))
        }
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .cache()

      val num2 = day2.count()
      if (num2 > 0) {
        val ret2 = day2.map(_._2).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println("day2", num2, ret2._1, ret2._2, ret2._1.toDouble / ret2._2.toDouble)
      }

      val sum = SetUserProfileTag.setUserProfileTag(day2.map(x => (x._1, 206, true)))
      sum.foreach(println)
    }
  }
}


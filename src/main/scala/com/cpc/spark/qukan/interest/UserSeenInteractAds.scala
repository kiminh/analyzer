package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  */
object UserSeenInteractAds {

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
      .appName("cpc get user interested category")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = args(0)
    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log where `date` = "%s" and adslotid in (%s)
      """.stripMargin.format(date, interactAdslotIds.mkString(","))

    val ipreg = """^[0-9.]+$""".r
    val uidRDD = spark.sql(stmt)
      .filter(r => ipreg.findFirstMatchIn(r.getString(0)).isEmpty) //filter ip

    println("num", uidRDD.count())
    println(uidRDD.take(10).mkString(" "))

    val conf = ConfigFactory.load()
    val sum = uidRDD.mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          var n2 = 0
          var n3 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            row =>
              n = n + 1
              val key = row.getString(0) + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(227)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {  //app来源
                    if (!has) {
                      has = true
                      n2 += 1
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n1 += 1  //浏览来源，且不在app来源的用户数
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              } else{
                n3 += 1
              }
          }
          Seq((n,n1,n2,n3)).iterator
      }
      println("update" + sum.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)))
  }
}


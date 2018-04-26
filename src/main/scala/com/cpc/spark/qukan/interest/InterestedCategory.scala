package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._

/**
  * Created by roydong on 08/11/2017.
  */
object InterestedCategory {

  val cates = Seq(
    100,
  101,
  102,
  103,
  104,
  105,
  106,
  107,
  108,
  109,
  110,
  111,
  112,
  113,
  114,
  115,
  116,
  117,
  118,
  119,
  120,
  121,
  122,
  123,
  124,
  125,
  126,
  127,
  128,
  129,
  130,
  131
  )

  val topCates = Seq(
    100,
    104,
    110,
    113,
    118,
    125,
    130
  )

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("cpc get user interested category")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    for (adclass <- topCates) {
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -2)
      var date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("adclass", adclass, date)
      val day1 = spark.sql(
        """
          |select uid, isclick from dl_cpc.cpc_union_log where `date` = "%s"
          |and round(ext['adclass'].int_value / 1e6,0) = %d and isshow = 1
        """.stripMargin.format(date, adclass)).rdd
        .map {
          u =>
            (u.getAs[String]("uid"), (u.getAs[Int]("isclick"), 1))
        }
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

      val num1 = day1.count()
      if (num1 > 0) {
        val ret1 = day1.map(_._2).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println("day1", num1, ret1._1, ret1._2, ret1._1.toDouble / ret1._2.toDouble)
      }

      cal.add(Calendar.DATE, 1)
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val day2 = spark.sql(
        """
          |select uid,isclick from dl_cpc.cpc_union_log where `date` = "%s"
          |and round(ext['adclass'].int_value / 1e6,0) = %d and isshow = 1
        """.stripMargin.format(date, adclass)).rdd
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

      val click1 = day1.filter(_._2._1 > 0)
      println("click1", click1.count())
      val day = click1.join(day2)
      val num = day.count()
      if (num > 0) {
        val ret = day.map(_._2._2).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println("and", num, ret._1, ret._2, ret._1.toDouble / ret._2.toDouble)
      }


      val conf = ConfigFactory.load()
      val sum = day2.filter(_._2._1 > 0).mapPartitions {
        p =>
          var n = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            row =>
              n = n + 1
              val key = row._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(adclass)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      user.setInterestedWords(i, in)
                      has = true
                    } else {
                      user.removeInterestedWords(i)
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(n).iterator
      }
      day2.unpersist()
      println("update", sum.sum())
    }

    /*
    cal.add(Calendar.DATE, 1)
    date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val day3 = spark.sql(
      """
        |select * from dl_cpc.cpc_union_log where `date` = "%s"
        |and ext['adclass'].int_value = %d and isshow = 1
      """.stripMargin.format(date, adclass)).as[UnionLog].rdd
      .map {
        u =>
          (u.uid, u.isclick)
      }
      */
  }
}


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
                  .setTag(230)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      has = true
                      n2 += 1
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n1 += 1
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

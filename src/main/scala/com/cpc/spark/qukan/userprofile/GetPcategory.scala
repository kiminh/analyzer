package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile
import com.redis.serialization.Parse.Implicits._

/**
  * Created by Roy on 2017/5/17.
  */
object GetPcategory {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before> <int>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get user pcategory [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    //user preferred type
    val memberDeviceId = ctx.sql(
      """
        |select member_id,device_code from gobblin.qukan_p_member_info where day = "%s"
      """.stripMargin.format(day))
      .rdd
      .map {
        x =>
          try {
            val id = x.getLong(0)
            val uid = x.getString(1)
            if (id > 0 && uid.length > 0) {
              (id, (uid, 0L))
            } else {
              null
            }
          } catch {
            case e: Exception => null
          }
      }
      .filter(_ != null)

    val memberPcate = ctx.sql(
      """
        |select member_id,type from algo_lechuan.user_preferred_type
      """.stripMargin)
      .rdd
      .map {
        x =>
          try {
            val id = x.getInt(0).toLong
            val cate = x.getLong(1)
            if (id > 0 && cate > 0) {
              (id, ("", cate))
            } else {
              null
            }
          } catch {
            case e: Exception => null
          }
      }
      .filter(_ != null)

    val pcateRdd = memberDeviceId.union(memberPcate)
      .reduceByKey {
        (x, y) =>
          var uid = ""
          var cate = 0L
          if (x._1.length > 0) {
            uid = x._1
          } else {
            uid = y._1
          }
          if (x._2 > 0) {
            cate = x._2
          } else {
            cate = y._2
          }
          (uid, cate)
      }
      .filter(x => x._2._1.length > 0 && x._2._2 > 0)
      .map(_._2)

    var caten = 0
    val v = pcateRdd
      .mapPartitions {
        p =>
          var n = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            x =>
              val key = x._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer)
                if (user.getPcategory != x._2.toInt) {
                  val u = user.toBuilder()
                    .setPcategory(x._2.toInt)
                    .build()
                  redis.setex(key, 3600 * 24 * 7, u.toByteArray)
                  n = n + 1
                }
              }
          }
          Seq((0, n)).iterator
      }
      .reduceByKey((x, y) => x + y)
      .take(1)
      .foreach(x => caten = x._2)

    println("update", caten)
    ctx.stop()
  }
}

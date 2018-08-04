package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.anal.ConditionTouchedUV.redis
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{UserProfile, InterestItem}

import scala.util.control.Breaks

/**
  * Created by
  */
object GetNewUser2 {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get new user")
      .enableHiveSupport()
      .getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val unionDate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -6)
    val sevenDay = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, - 7)
    val toWeek = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sql1 = " SELECT field[\"device\"].string_type ,day from dl_lechuan.qukan_daily_new_user_p "
    val sql2 =  """
            SELECT uid,sum(isclick),sum(isshow)  from dl_cpc.cpc_union_log where
            `date` = '%s' and isfill > 0 and  media_appsid in("80000001","80000002")
             GROUP BY uid
           """.stripMargin.format(unionDate)

    println("sql1" + sql1)
    println("sq2:"+sql2)
    println("toWeek [%s]".format(toWeek))
    println("unionDate [%s]".format(unionDate))

    val rdd1 = ctx.sql(sql1).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          val day  = x(1).toString()
          (uid,day)

      }.reduceByKey((x, y) => x).cache()
    val rdd2 = ctx.sql(sql2).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          (uid, 1)
      }
      .distinct()
    val all = rdd1.join(rdd2).map{
      case (uid, (day,x)) =>
        (uid, day)
    }
    all.take(10).foreach(println)
    val toSet = all.flatMap{
      case (uid, day) =>
        if (sevenDay <= day) {
          Seq()
        } else {
          if (toWeek > day) {
            Seq((uid, 236, true), (uid, 228, true))
          } else {
            Seq((uid, 228, true))
          }
        }
    }
    SetUserProfileTag.setUserProfileTag(toSet)
    val sum = all.repartition(200)
      .mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        var n3 = 0
        var n4 = 0
        var n5 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        val loop = new Breaks
        p.foreach {
          case (uid,day) =>
            n1 = n1 + 1
            var deviceid : String = uid
            var user : UserProfile.Builder = null
            val key = deviceid + "_UPDATA"
            var have = false
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer != null) {
              n1 += 1
              loop.breakable{
                user = UserProfile.parseFrom(buffer).toBuilder
                var idx = 0
                while(idx < user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(idx)
                  if (w.getTag == 228) {
                    user.removeInterestedWords(idx)
                    have = true
                  } else {
                    idx += 1
                  }
                  if (idx == user.getInterestedWordsCount) {
                    loop.break()
                  }
                }
              }
              if (sevenDay > day) {
                val in = InterestItem.newBuilder()
                  .setTag(228)
                  .setScore(100)
                user.addInterestedWords(in)
                n2 += 1
                if (!have) {
                  n4 += 1
                }
              }  else {
                n3 += 1
              }
              //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)

            }
        }
        Seq((0, n1), (1, n2), (2, n3),(3, n4),(4, n5)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    var n4 = 0
    var n5 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(5)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else if (x._1 == 1) {
            n2 = x._2
          } else if (x._1 == 2) {
            n3 = x._2
          } else if (x._1 == 3) {
            n4 = x._2
          } else if (x._1 == 4) {
            n5 = x._2
          }
      }
    redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(3)

    val key = "touched_uv_percent_old_or_new_user_2"
    val key2 = "touched_uv_percent_old_or_new_user_1"

    redis.set(key, "%.8f".format(n2.toDouble/n1.toDouble))
    redis.set(key2, "%.8f".format(n3.toDouble/n1.toDouble))
    println("total: %d new-7-user: %d old: %d update:%d".format(n1, n3, n2, n4))
    ctx.stop()
  }
}



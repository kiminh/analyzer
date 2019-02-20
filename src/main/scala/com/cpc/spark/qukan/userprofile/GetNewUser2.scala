package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

import scala.util.control.Breaks

/**
  * Created by
  */
@deprecated
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
    SetUserProfileTag.SetUserProfileTagInHiveDaily(toSet)
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
            if (buffer != null && day != null) {
              n1 += 1
              val user = UserProfile.parseFrom(buffer).toBuilder
              user.setUserCreateTime(day)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)

            }
        }
        Seq((0, n1)).iterator
    }.reduceByKey(_+_)
    println(sum.toLocalIterator)
    ctx.stop()
  }
}



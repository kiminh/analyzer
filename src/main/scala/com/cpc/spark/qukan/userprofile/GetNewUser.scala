package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.anal.ConditionTouchedUV.redis
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

/**
  * Created by
  */
@deprecated
object GetNewUser {

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
    cal.add(Calendar.DATE, -44)
    val toDay = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sql1 = " SELECT field[\"device\"].string_type ,day from dl_lechuan.qukan_daily_new_user_p "
    val sql2 =  """
            SELECT uid,sum(isclick),sum(isshow)  from dl_cpc.cpc_union_log where
            `date` = '%s' and isfill > 0 and  media_appsid in("80000001","80000002")
             GROUP BY uid
           """.stripMargin.format(unionDate)

    println("sql1" + sql1)
    println("sq2:"+sql2)
    println("toDay [%s]".format(toDay))
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
    val all = rdd1.join(rdd2).map{
      case (uid, (day,x)) =>
        (uid, day)
    }
    val sum = all.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        var n3 = 0
        var n4 = 0
        var n5 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
         case (uid,day) =>
            n1 = n1 + 1
            var deviceid : String = uid
            var user : UserProfile.Builder = null
            val key = deviceid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)

            if(toDay > day){ //老用户
              n2 = n2 + 1
              if (buffer != null) {
                user = UserProfile.parseFrom(buffer).toBuilder
                if(user.getNewUser == 1){
                  n4 = n4 + 1
                  user.setNewUser(0)
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                }
              }
            }else{//新用户
              n3 = n3 + 1
              if (buffer == null) {
                user = UserProfile.newBuilder().setDevid(deviceid)
              }else {
                user = UserProfile.parseFrom(buffer).toBuilder
                if(user.getNewUser != 1){
                  n5 = n5 + 1
                }
              }
              user.setNewUser(1)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
            }
        }
        Seq((0, n1), (1, n2), (3, n3),(4, n4),(5, n5)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    var n4 = 0
    var n5 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else if (x._1 == 1) {
            n2 = x._2
          } else if (x._1 == 3) {
            n3 = x._2
          } else if (x._1 == 4) {
            n4 = x._2
          } else if (x._1 == 5) {
            n5 = x._2
          }
      }
    redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(3)


    val key = "touched_uv_percent_old_or_new_user_2"
    val key2 = "touched_uv_percent_old_or_new_user_1"

    redis.set(key, "%.8f".format(n2.toDouble/n1.toDouble))
    redis.set(key2, "%.8f".format(n3.toDouble/n1.toDouble))
    println("total: %d older: %d newUser: %d olderUpdateUser: %d newUpdateUser: %d".format(n1, n2, n3, n4, n5))
    ctx.stop()
  }
}



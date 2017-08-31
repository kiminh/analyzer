package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

/**
  * Created by
  */
object GetUserAntispamV2 {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserAntispam <dayBefore> <ctr> <show>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    val num = args(1).toInt
    val showNum = args(2).toInt
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val day2 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc get user antispam v2 from [%s] to [%s] and ctr [%s] show[%s]".format(day1, day2, num, showNum))
      .enableHiveSupport()
      .getOrCreate()
    val sqltext =  "SELECT uid, sum(isclick) as click, sum(isshow) as show from dl_cpc.cpc_union_log where `date` in( '"+day1+"', '"+day2+"') and adslot_type =1 GROUP BY uid  "
    println("sql:" + sqltext)
    val rddOne = ctx.sql(sqltext).rdd.map{
      x =>
        val uid : String =  x(0).toString()
        val click : Int = x(1).toString().toInt
        val show : Int = x(2).toString().toInt
        (uid,(click, show))
    }.map{
      case  (uid,(click, show)) =>
        var ctr = click.toFloat/show.toFloat * 100
        (uid,click, show, ctr.toInt)
    }
    println("rddOne-count:"+ rddOne.count())


    val  rddTwo = rddOne.filter(x => x._4 > num && x._3 > showNum)
    println("rddTwo-count:"+ rddTwo.count())
    rddTwo.take(10).foreach(x => println("user:" + x))
    val sum =  rddTwo.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          case (uid,click, show, ctr) =>
            n1 = n1 + 1
            var user : UserProfile.Builder = null
            val key = uid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer == null) {
              user = UserProfile.newBuilder().setDevid(uid)
            }else {
              user = UserProfile.parseFrom(buffer).toBuilder
              if(user.getAntispam != 1){
                n2 = n2 + 1
              }
            }
            user.setAntispam(1)
            redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
        }
        Seq((0, n1), (1, n2)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else {
            n2 = x._2
          }
      }
    println("total: %d updated: %d".format(n1, n2))
    ctx.stop()
  }
}



package com.cpc.spark.small.tool

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile
import com.redis.serialization.Parse.Implicits._
/**
  * Created by wanli on 2017/8/4.
  */
object CleanAntispam {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)
    val dateDay = args(0)
    val ctx = SparkSession.builder()
      .appName("clean antispam ")
      .enableHiveSupport()
      .getOrCreate()
   val sql =  """
             SELECT uid from dl_cpc.cpc_union_log where `date` in(%s) and ext['antispam'].int_value = 1 GROUP BY uid
           """.stripMargin.format(dateDay)
    println("sql:"+sql)
    val conf = ConfigFactory.load()
    val sum = ctx.sql(sql).rdd.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            var deviceid : String = x(0).toString()
            var user : UserProfile.Builder = null
            val key = deviceid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer != null) {
              user = UserProfile.parseFrom(buffer).toBuilder
              if(user.getAntispam == 1){
                n2 = n2 + 1
                user.setAntispam(0)
             //   redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
            }

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
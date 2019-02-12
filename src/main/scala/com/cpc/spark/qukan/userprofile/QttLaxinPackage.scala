package com.cpc.spark.qukan.userprofile

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

/**
  * Created by roydong on 11/07/2018.
  */
@deprecated
object QttLaxinPackage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("qtt laxin package")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = args(0)
    val lxrdd = spark.read.parquet("/warehouse/bdm.db/qukan_daily_new_user_p/day=%s".format(date))
      .map {
        r =>
          val devid = r.getAs[String]("device")
          val lxs = r.getAs[String]("is_nlx")
          val pkg = r.getAs[Int]("app_package")
          var lx = 0
          if (lxs == "内拉新") {
            lx = 1
          } else if (lxs == "外拉新") {
            lx = 2
          }
          (devid, lx, pkg)
      }

    lxrdd.take(5).foreach(println)
    val sum = lxrdd.coalesce(200)
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          val conf = ConfigFactory.load()
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            x =>
              val key = x._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val qtt = user.getQttProfile.toBuilder
                n = n + 1
                if (qtt.getDevid != x._1 || qtt.getLxPackage != x._3) {
                  qtt.setDevid(x._1)
                  qtt.setLxType(x._2)
                  qtt.setLxPackage(x._3)
                  user.setQttProfile(qtt)
                  n1 = n1 + 1
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                }
              }
          }
          Seq(n1).iterator
      }
    println("update", sum.rdd.sum)
  }
}

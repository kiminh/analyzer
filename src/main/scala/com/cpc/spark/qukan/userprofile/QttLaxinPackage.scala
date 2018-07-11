package com.cpc.spark.qukan.userprofile

import org.apache.spark.sql.SparkSession
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  * Created by roydong on 11/07/2018.
  */
object QttLaxinPackage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("qtt laxin package")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = args(0)

    val sql =
      """
        |select device, is_nlx, lx_way, app_package from bdm.qukan_daily_new_uesr_p
        |where day = "%s" and is_dubious = 0
      """.stripMargin.format(date)

    val sum = spark.sql(sql).coalesce(200)
      .mapPartitions {
        p =>
          var n = 0
          val conf = ConfigFactory.load()
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            r =>
              val devid = r.getString(0)
              val lxs = r.getString(1)
              val pkg = r.getInt(3)

              var lx = 0
              if (lxs == "内拉新") {
                lx = 1
              } else if (lxs == "外拉新") {
                lx = 2
              }

              val key = devid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val qtt = user.getQttProfile.toBuilder
                qtt.setDevid(devid)
                qtt.setLxType(lx)
                qtt.setLxPackage(pkg)
                user.setQttProfile(qtt)
                n = n + 1
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(n).iterator
      }

    println("update", sum.rdd.sum)
  }
}

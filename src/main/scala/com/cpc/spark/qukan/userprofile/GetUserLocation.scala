package com.cpc.spark.qukan.userprofile

import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile
@deprecated
object GetUserLocation {
    def main(args: Array[String]): Unit = {
        Logger.getRootLogger.setLevel(Level.WARN)
        val day = args(0).toString
        val conf = ConfigFactory.load()

        val ctx = SparkSession.builder()
            .appName("cpc get user location %s".format(day))
            .enableHiveSupport()
            .getOrCreate()

        val sql =
            """
              |SELECT info.device_code, city.province, city.city
              |from rpt_qukan.analysis_province_city_p as city LEFT JOIN dl_cpc.qukan_p_member_info as info on city.id = info.member_id
              |WHERE (city.city != "" or city.province != "") and info.device_code != "" and city.day = "%s" and info.day = "%s"
            """.stripMargin.format(day, day)

        println("sql:" + sql)

        val rdd = ctx.sql(sql).rdd
            .map {
                x =>
                    val devCode: String = x(0).toString
                    var province = x(1).toString
                    var city = x(2).toString
                    province = province.stripSuffix("省")
                    province = province.stripSuffix("市")
                    if (!(city.equals("新北市") || city.equals("台北市") || city.equals("高雄市") || city.equals("台中市") || city.equals("新竹市") || city.equals("桃园市") || city.equals("基隆市")
                        || city.equals("嘉义市") || city.equals("台南市") || city.equals("吉林市"))) {
                        city = city.stripSuffix("市")
                    }
                    (devCode, (province, city))
            }.reduceByKey {
            (x, y) => (x._1, x._2)
        }.cache()

//        val data = rdd.collect()
//        for (d <- data) {
//            println(d.toString())
//        }

        val sum = rdd.mapPartitions {
            p =>
                var n1 = 0
                var n2 = 0
                var n3 = 0
                val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
                p.foreach {
                    case (devCode, (province, city)) =>
                        n1 = n1 + 1
                        val key = devCode + "_UPDATA"
                        val buffer = redis.get[Array[Byte]](key).orNull
                        var user: UserProfile.Builder = null
                        if (buffer == null) {
                            user = UserProfile.newBuilder().setDevid(devCode)
                            n2 = n2 + 1
                        } else {
                            user = UserProfile.parseFrom(buffer).toBuilder
                        }
                        val u = user.build()
                        if (u.getProvince != province || u.getCity != city) {
                            n3 = n3 + 1
                            user = user.setProvince(province).setCity(city)
                            redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                        }
                }
                Seq((0, n1), (1, n2), (2, n3)).iterator
        }

        var n1 = 0
        var n2 = 0
        var n3 = 0
        sum.reduceByKey((x, y) => x + y)
            .take(3)
            .foreach {
                x =>
                    if (x._1 == 0) {
                        n1 = x._2
                    } else if (x._1 == 1) {
                        n2 = x._2
                    } else {
                        n3 = x._2
                    }
            }

        println("total: %d new: %d update: %d".format(n1, n2, n3))
        ctx.stop()
    }

}

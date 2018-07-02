package main.scala.com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.control._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Row, SparkSession}

object TagUserByZfb {
  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .appName("Tag bad uid")
      .enableHiveSupport()
      .getOrCreate()

    val zfb = spark.read.parquet("/user/cpc/qtt-zfb/%s".format(days)).rdd.map {
      r =>
        val did = r.getAs[String]("did")
        val s_sex = r.getAs[String]("sex")
        val birth = (2018 - r.getAs[String]("birth").toInt) / 10000
        var age = 224
        if (birth < 22) {
          age = 224
        } else {
          age = 225
        }
        var sex = 2
        if (s_sex == "m") {
          sex = 1
        }
        (did, sex, age)
    }
    val conf = ConfigFactory.load()
    zfb.take(1).foreach {
      r =>
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        val key = r._1 + "_UPDATA"
        val buffer = redis.get[Array[Byte]](key).orNull
        if (buffer != null) {
          val user = UserProfile.parseFrom(buffer).toBuilder
          for (i <- 0 until user.getInterestedWordsCount) {
            val in = user.getInterestedWords(i)
            println(i + " " + in.getTag + " " + in.getScore)
          }
          println("after")
          user.removeInterestedWords(user.getInterestedWordsCount / 2)
          for (i <- 0 until user.getInterestedWordsCount) {
            val in = user.getInterestedWords(i)
            println(i + " " + in.getTag + " " + in.getScore)
          }
        }

    }
    /*
    val sum = zfb
      .mapPartitions {
        p =>
          var age_n = 0  //age插入个数
          var sex_n = 0  //sex插入个数
          var age_m = 0  //age冲突个数
          var sex_m = 0  //sex冲突个数
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          val loop = new Breaks

          p.foreach {
            r =>
              val key = r._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(r._3)
                  .setScore(100)
                var has = false
                var conflict = false
                loop.breakable{
                  var idx = 0
                  while(true) {
                    val w = user.getInterestedWords(idx)
                    if (w.getTag == 224 || w.getTag == 225) {
                      has = true
                      if (w.getTag != in.getTag) {
                        conflict = true
                      }
                      user.removeInterestedWords(idx)
                    } else {
                      idx += 1
                    }
                    if (idx == user.getInterestedWordsCount) {
                      loop.break()
                    }
                  }
                }
                user.addInterestedWords(in)
                if (!has) {
                  age_n += 1
                }
                if (conflict) {
                  age_m += 1
                }
                if (r._2 != user.getSex) {
                  sex_m += 1
                  user.setSex(sex)
                }
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((age_n, age_m, sex_m)).iterator
      }
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println("###" + sum._1+ "###" + sum._2 )
  */
  }

}

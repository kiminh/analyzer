package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.userprofile.SetUserProfileTag
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.util.control._

/***
  * Create by myt on 19/06/2018
  *
  */
@deprecated
object TagUserByZfb {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val is_set = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag user by zfb")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log where `date` = "%s" and media_appsid  in ("80000001", "80000002")
      """.stripMargin.format(sdate)

    val uv = spark.sql(stmt).rdd.map {
      r =>
        val did = r.getAs[String](0)
        (did)
    }.distinct().toDF("did")
    val zfb = spark.read.parquet("/user/cpc/qtt-zfb/%s".format(days)).join(uv, Seq("did")).rdd.map {
      r =>
        val did = r.getAs[String]("did")
        val s_sex = r.getAs[String]("sex")
        val birth = 2018 - (r.getAs[String]("birth").toInt / 10000)
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
    println("student number:")
    println(zfb.filter(x => x._3 == 224).count())
    println("m student number:")
    println(zfb.filter(x => x._3 == 224 && x._2 == 1).count())
    val age = zfb.flatMap {
      x =>
        if (x._3 == 224) {
          Seq((x._1, 224, true), (x._1, 225, false), (x._1, 239, true), (x._1, 240, false))
        } else {
          Seq((x._1, 224, false), (x._1, 225, true), (x._1, 239, false), (x._1, 240, true))
        }
    }
    val ret = SetUserProfileTag.SetUserProfileTagInHiveDaily(age)
    ret.foreach(println)

    val conf = ConfigFactory.load()
    val sum = zfb
      .mapPartitions {
        p =>
          var age_n = 0  //age插入个数
          var age_r = 0  //sex插入个数
          var age_m = 0  //age冲突个数
          var sex_m = 0  //sex冲突个数
          var new_young = 0
          var add_224 = 0
          var newU = 0
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
                var age_224 = false
                var age_225 = false
                val newuser = user.getNewUser
                if (newuser == 1) {
                  newU += 1
                }
                if (newuser == 1 && r._3 == 224){
                  new_young += 1
                }
                loop.breakable{
                  var idx = 0
                  while(idx < user.getInterestedWordsCount) {
                    val w = user.getInterestedWords(idx)
                    if (w.getTag == 224 || w.getTag == 225) {
                      if (w.getTag == 224) age_224 = true
                      if (w.getTag == 225) age_225 = true
                      has = true
                      if (w.getTag != in.getTag) {
                        conflict = true
                      }
                      idx += 1
                    } else {
                      idx += 1
                    }
                    if (idx == user.getInterestedWordsCount) {
                      loop.break()
                    }
                  }
                }
                //user.addInterestedWords(in)
                if (!has) {
                  if (in.getTag == 224) {
                    add_224 += 1
                  }
                  age_n += 1
                }
                if (conflict) {
                  age_m += 1
                }
                if (r._2 != user.getSex && user.getSex != 0) {
                  sex_m += 1
                  user.setSex(r._2)
                }
                if (age_224 && age_225) {
                  age_r += 1
                }
                if (is_set) {
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                }
              }
          }
          Seq((age_n, age_m, sex_m, age_r, add_224, new_young, newU)).iterator
      }
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3,x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7))
    println("age_newupdate:" + sum._1+ " age_conflict:" + sum._2 + " sex_conflict:" + sum._3 + " age_both_taged:" + sum._4 + "add_244:" + sum._5 + " new_young:" + sum._6 + " newUser:" + sum._7)
  }

}

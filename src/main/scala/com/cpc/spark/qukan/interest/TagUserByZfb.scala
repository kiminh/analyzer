package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.train.LRIRModel
import com.cpc.spark.qukan.parser.HdfsParser
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.cpc.spark.qukan.parser.HdfsParser
import userprofile.Userprofile.{InterestItem, UserProfile}
import scala.util.control._

object TagUserByZfb {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val spark = SparkSession.builder()
      .appName("Tag user by zfb")
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
    val sum = zfb
      .mapPartitions {
        p =>
          var age_n = 0  //age插入个数
          var age_r = 0  //sex插入个数
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
                var age_224 = false
                var age_225 = false
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
                  user.setSex(r._2)
                }
                if (age_224 && age_225) {
                  age_r += 1
                }
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((age_n, age_m, sex_m)).iterator
      }
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    println("###" + sum._1+ "###" + sum._2 + "###" + sum._3)

  }

}

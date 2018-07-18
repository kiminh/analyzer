package com.cpc.spark.qukan.userprofile

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._

import scala.util.control._
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.spark.rdd.RDD
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.collection.mutable

/**
  * Created by YuntaoMa on 18/07/2018
  *
  * Discreption
  * setUserProfileTag(spark : SparkSession, in : RDD[(String, Int, Boolean)]
  * spark :  Context
  * in : A RDD type list uid, tag_number, operation(insert or delete)
  *
  * Return
  * Array[(String, Int)]  contains 4 data counts in ("total", "hit", "insert", "delete")
  */


class SetUserProfileTag {
  def setUserProfileTag (spark : SparkSession, in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    import spark.implicits._
    val conf = ConfigFactory.load()
    val sum = in.repartition(500)
      .mapPartitions{
        p =>
          var del = 0
          var ins = 0
          var hit = 0
          var tot = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          val loop = new Breaks
          p.foreach{
            x =>
              tot += 1
              val key = x._1 + "_UPDATAT"
              val buffer = redis.get[Array[Byte]](key).orNull
              var isDel = false
              var isIns = false
              if (buffer != null) {
                hit += 1
                val user = UserProfile.parseFrom(buffer).toBuilder
                val interest = InterestItem.newBuilder()
                  .setTag(x._2)
                  .setScore(100)
                loop.breakable {
                  var idx = 0
                  while (idx < user.getInterestedWordsCount) {
                    val w = user.getInterestedWords(idx)
                    if (w.getTag == x._2) {
                      isDel = true
                      user.removeInterestedWords(idx)
                    } else {
                      idx += 1
                    }
                    if (idx == user.getInterestedWordsCount) {
                      loop.break()
                    }
                  }
                }
                if (x._3) {
                  if (!isDel) {
                    ins += 1
                  }
                  user.addInterestedWords(interest)
                } else {
                  if (isDel) {
                    del += 1
                  }
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del)).iterator
      }.reduceByKey(_+_)
      .sortBy(_._1)

    sum.toLocalIterator.toArray[(String, Int)]
  }

  def TestsetUserProfileTag (spark : SparkSession, in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    import spark.implicits._
    val conf = ConfigFactory.load()
    val sum = in.repartition(500)
      .mapPartitions{
        p =>
          var del = 0
          var ins = 0
          var hit = 0
          var tot = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          val loop = new Breaks
          p.foreach{
            x =>
              tot += 1
              val key = x._1 + "_UPDATAT"
              val buffer = redis.get[Array[Byte]](key).orNull
              var isDel = false
              var isIns = false
              if (buffer != null) {
                hit += 1
                val user = UserProfile.parseFrom(buffer).toBuilder
                val interest = InterestItem.newBuilder()
                  .setTag(x._2)
                  .setScore(100)
                loop.breakable {
                  var idx = 0
                  while (idx < user.getInterestedWordsCount) {
                    val w = user.getInterestedWords(idx)
                    if (w.getTag == x._2) {
                      isDel = true
                      user.removeInterestedWords(idx)
                    } else {
                      idx += 1
                    }
                    if (idx == user.getInterestedWordsCount) {
                      loop.break()
                    }
                  }
                }
                if (x._3) {
                  if (!isDel) {
                    ins += 1
                  }
                  user.addInterestedWords(interest)
                } else {
                  if (isDel) {
                    del += 1
                  }
                }
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del)).iterator
      }.reduceByKey(_+_)
      .sortBy(_._1)

    sum.toLocalIterator.toArray[(String, Int)]
  }

}

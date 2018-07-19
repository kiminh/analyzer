package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
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


object SetUserProfileTag {
  def setUserProfileTag (spark : SparkSession, in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    import spark.implicits._
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
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
          var ret = Seq[(String, Int)]()
          val cnt = p.foreach{
            x =>
              tot += 1
              val key = x._1 + "_UPDATA"
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
                  isIns = true
                } else {
                  if (isDel) {
                    del += 1
                  }
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
              if (isIns) {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), 1)
              }else if (isDel) {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), -1)
              } else {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), 0)
              }
          }
          (Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del)) ++ ret).iterator
      }.reduceByKey(_+_)
      .sortBy(_._1)
    sum.toDF("name", "sum").write.mode(SaveMode.Append).parquet("/user/cpc/uid-tag-number/%s".format(date))
    sum.toLocalIterator.toArray[(String, Int)]

  }

  def testSetUserProfileTag (spark : SparkSession, in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    import spark.implicits._
    in.map {
      x =>
        ((x._2, x._3), 1)
    }.reduceByKey(_+_)
      .toLocalIterator
      .foreach(println)
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
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
          var ret = Seq[(String, Int)]()
          val cnt = p.foreach{
            x =>
              tot += 1
              val key = x._1 + "_UPDATA"
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
                  isIns = true
                } else {
                  if (isDel) {
                    del += 1
                  }
                }
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
              if (isIns) {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), 1)
              }else if (isDel) {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), -1)
              } else {
                ret = ret :+ ("uid_num_by_tag_%s".format(x._2), 0)
              }
          }
          (Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del)) ++ ret).iterator
      }.reduceByKey(_+_)
      .sortBy(_._1)

    sum.toDF("name", "sum").write.mode(SaveMode.Append).parquet("/user/cpc/uid-tag-number/test-%s".format(date))
    sum.toLocalIterator.toArray[(String, Int)]
  }

  def main(args: Array[String]): Unit = {
    val isTest = args(0).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag bad uid")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val raw = {
      if (isTest) {
        println("today")
        spark.read.parquet("/user/cpc/uid-tag-number/%s".format(today))
      } else {
        println("yesterday")
        spark.read.parquet("/user/cpc/uid-tag-number/test-%s".format(yesterday))
      }
    }
    raw.rdd.map {
      r =>
        val tag = r.getAs[String](0)
        val cnt = r.getAs[Int](1)
        (tag, cnt)
    }.reduceByKey(_+_)
      .toLocalIterator
      .foreach(println)
  }

}

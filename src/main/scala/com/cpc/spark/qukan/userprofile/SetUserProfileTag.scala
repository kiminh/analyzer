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
  def setUserProfileTag (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
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
      }.reduceByKey(_+_).toLocalIterator
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    sum.foreach{
      x =>
        val key = x._1 + "_%s".format(date)
        if (x._1.contains("uid")) {
          val buffer = redis.get[Int](x._1)
          if (buffer != None) {
            val ret = buffer.get
            if (ret != null) {
              redis.setex(key, 3600 * 24 * 30, (ret + x._2).toString)
              println("set %s as %s".format(key, (ret + x._2).toString))
            }
          } else {
            redis.setex(key, 3600 * 24 * 30, x._2.toString)
            println("set %s as %s".format(key, (x._2).toString))
          }
        }

    }
    sum.toArray[(String, Int)]

  }

  def testSetUserProfileTag (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
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
      }.reduceByKey(_+_).toLocalIterator
    sum.foreach(println)
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    sum.foreach{
      x =>
        val key = x._1 + "_%s_test".format(date)
        if (x._1.contains("uid")) {
          val buffer = redis.get[Int](x._1)
          if (buffer != None) {
            val ret = buffer.get
            redis.setex(key, 3600 * 24 * 30, (ret + x._2).toString)
            println("set %s as %s".format(key, (ret + x._2).toString))
          } else {
            redis.setex(key, 3600 * 24 * 30, x._2.toString)
            println("set %s as %s".format(key, (x._2).toString))
          }
        }

    }
    sum.toArray[(String, Int)]
  }

  def main(args: Array[String]): Unit = {
    val tagList = Array[Int](201, 202, 203, 204, 205, 206, 207, 208, 209, 212, 216, 218, 219, 220, 221, 222,
      223, 224, 225, 226, 227, 228, 230, 231, 233, 234, 235)
    val isTest = args(0).toBoolean
    val spark = SparkSession.builder()
      .appName("count userprofile tag")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val redis2save = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis2save.select(3)
    tagList.foreach{
      x =>
        val key = {
          if (isTest) {
            "uid_num_by_tag_" + x + "_%s_test".format(today)
          } else {
            "uid_num_by_tag_" + x + "_%s".format(yesterday)
          }
        }
        val buffer = redis.get[Int](key)
        println(key, buffer)
        if (buffer != None) {
          val ret = buffer.get
          if (ret != null && isTest) {
            redis2save.setex("uid_num_by_tag_%s_test".format(x), 3600 * 24 * 30, ret.toString)
            println("set %s as %s".format("uid_num_by_tag_%s_test".format(x), ret.toString))
          } else if (ret != null && !isTest){
            redis2save.setex("uid_num_by_tag_%s".format(x), 3600 * 24 * 30, ret.toString)
            println("set %s as %s".format("uid_num_by_tag_%s".format(x), ret.toString))
          }
        } else {
          null
        }
    }

  }

}

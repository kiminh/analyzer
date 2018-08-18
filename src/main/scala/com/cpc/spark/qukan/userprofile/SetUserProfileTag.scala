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
    val sum = in.repartition(200)
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
              }
          }
          (Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del)) ++ ret).iterator
      }.reduceByKey(_+_).toLocalIterator
    //sum.foreach(println)
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    sum.foreach{
      x =>
        val key = x._1 + "_%s".format(date)
        println(x)
        if (x._1.contains("uid")) {
          val buffer = redis.get[Int](key)
          println("get %s return %s".format(key, buffer))
          if (buffer != None) {
            val ret = buffer.get
            println("origin: %s".format(ret))
            redis.setex(key, 3600 * 24 * 30, (ret + x._2).toString)
            println("set add %s as %s".format(key, (ret + x._2).toString))
          } else {
            redis.setex(key, 3600 * 24 * 30, x._2.toString)
            println("set new %s as %s".format(key, (x._2).toString))
          }
        }

    }
    sum.toArray[(String, Int)]

  }

  def testSetUserProfileTag (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val sum = in.repartition(200)
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
    sum.foreach(println)
//    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
//    sum.foreach{
//      x =>
//        val key = x._1 + "_%s".format(date)
//        println(x)
//        if (x._1.contains("uid")) {
//          val buffer = redis.get[Int](key)
//          println("get %s return %s".format(key, buffer))
//          if (buffer != None) {
//            val ret = buffer.get
//            println("origin: %s".format(ret))
//            redis.setex(key, 3600 * 24 * 30, (ret + x._2).toString)
//            println("set add %s as %s".format(key, (ret + x._2).toString))
//          } else {
//            redis.setex(key, 3600 * 24 * 30, x._2.toString)
//            println("set new %s as %s".format(key, (x._2).toString))
//          }
//        }
//
//    }
    sum.toArray[(String, Int)]
  }
  def SetUserProfileTagInHiveHourly (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ft = in.map(x => x._2).distinct().toLocalIterator
    val rs = ft.map{
      tag =>
        in.filter(_._2 == tag).map{x => (x._1, x._3)}.toDF("uid", "operation").coalesce(20)
          .write.mode(SaveMode.Overwrite).parquet("/warehouse/dl_cpc.db/cpc_userprofile_tag_hourly/%s/%s/%s".format(date, hour, tag))
        val sql =
          """
            |ALTER TABLE dl_cpc.cpc_userprofile_tag_hourly add if not exists PARTITION (`date` = "%s" , `hour` = "%s", `tag` = "%s")  LOCATION
            |       '/warehouse/dl_cpc.db/cpc_userprofile_tag_hourly/%s/%s/%s'
            |
                """.stripMargin.format(date, hour, tag, date, hour, tag)
        spark.sql(sql)
        (sql, in.filter(_._2 == tag).count().toInt)
    }
    rs.foreach(println)
    rs.toArray
  }



  def SetUserProfileTagInHiveDaily (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ft = in.map(x => x._2).distinct().toLocalIterator
    val rs = ft.map{
      tag =>
        in.filter(_._2 == tag).map{x => (x._1, x._3)}.toDF("uid", "operation").coalesce(20)
          .write.mode(SaveMode.Append).parquet("/warehouse/dl_cpc.db/cpc_userprofile_tag_daily/%s/%s".format(date, tag))
        val sql =


          """
            |ALTER TABLE dl_cpc.cpc_userprofile_tag_daily add if not exists PARTITION (`date` = "%s" , `tag` = "%s")  LOCATION
            |       '/warehouse/dl_cpc.db/cpc_userprofile_tag_daily/%s/%s'
            |
                """.stripMargin.format(date, tag, date, tag)
        spark.sql(sql)
        (sql, in.filter(_._2 == tag).count().toInt)
    }
    rs.foreach(println)
    rs.toArray
  }
  def SetUserProfileTagInHiveDaily_Append (in : RDD[(String, Int, Boolean)]) : Array[(String, Int)] = {
    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ft = in.map(x => x._2).distinct().toLocalIterator
    val rs = ft.map{
      tag =>
        in.filter(_._2 == tag).map{x => (x._1, x._3)}.toDF("uid", "operation").coalesce(20)
          .write.mode(SaveMode.Append).parquet("/warehouse/dl_cpc.db/cpc_userprofile_tag_daily/%s/%s".format(date, tag))
        val sql =
          """
            |APPEND TABLE dl_cpc.cpc_userprofile_tag_daily add if not exists PARTITION (`date` = "%s" , `tag` = "%s")  LOCATION
            |       '/warehouse/dl_cpc.db/cpc_userprofile_tag_daily/%s/%s'
            |
                """.stripMargin.format(date, tag, date, tag)
        //spark.sql(sql)
        (sql, in.filter(_._2 == tag).count().toInt)
    }
    rs.foreach(println)
    rs.toArray
  }
  def main(args: Array[String]): Unit = {
    val isTest = args(0).toBoolean
    val days = args(1).toInt
    val spark = SparkSession.builder()
      .appName("count userprofile tag")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -days)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val redis2save = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis2save.select(3)
    val stmt =
      """
        |select tag, count(distinct uid) from dl_cpc.cpc_userprofile_tag_daily
        |where `date` = "%s" and operation = true group by tag
      """.stripMargin.format(yesterday)
    val tagList = spark.sql(stmt).rdd.map {
      r =>
        ("uid_num_by_tag_" + r.getAs[Int](0), r.getAs[Long](1).toString)
    }.toLocalIterator
    tagList.foreach{
      x =>
        println(x._1, x._2)
        redis2save.setex(x._1, 3600 * 24 * 30, x._2)
    }

  }

}

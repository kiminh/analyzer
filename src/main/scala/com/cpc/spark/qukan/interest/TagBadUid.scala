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

/**
  * Created by YuntaoMa on 06/06/2018.
  */

object TagBadUid {
  def main(args: Array[String]): Unit = {
    val threshold = args(0).toInt
    val hour = args(1).toInt
    val time_span = args(2).toInt
    val spark = SparkSession.builder()
      .appName("Tag bad uid")
      .enableHiveSupport()
      .getOrCreate()


    val cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var sHour = "("
    for (i <- hour - time_span to hour - 1) {
      sHour += "'%02d',".format(i)
    }
    sHour += "'%02d')".format(hour)
    println(sHour)
    println(date)
    var stmt =
      """
        |SELECT searchid,uid,userid,ext
        |FROM dl_cpc.cpc_union_log
        |WHERE `date` = "%s" AND isshow = 1 AND (media_appsid = "80000001" OR media_appsid = "80000002")
      """.stripMargin.format(date)

    var rs1 = spark.sql(stmt).rdd
      .map {
        row =>
          val searchid = row.getString(0)
          val uid = row.getString(1)
          val userid = row.getInt(2)
          val ext = row.getMap[String, Row](3)
          val new_user = ext("qukan_new_user").getAs[Int]("int_value")

          (searchid, "user#" + userid + "u#" + uid)
          /*
          if (new_user == 1) {
            (searchid, "user#" + userid + "u#" + uid)
          }
          else {
            null
          }
          */
      }
        .filter(_ != null)
    rs1.take(10).foreach(println)

    stmt =
      """
        |SELECT searchid
        |FROM dl_cpc.cpc_union_trace_log
        |WHERE `date` = "%s" AND `hour` in %s AND trace_type like "active%%"
      """.stripMargin.format(date, sHour)

    val rs2 = spark.sql(stmt).rdd
        .map {
          row =>
            (row.getString(0), 1)
        }
        .reduceByKey((x, y) => x + y)

    val rs3 = rs1.join(rs2)
      .map(x => x._2)
      .reduceByKey((x, y) => x + y)
      .map {
        x =>
          (x._1.split("u#") (1), x._2)
      }
      .reduceByKey((x, y) => x.max(y))

    val stage = rs3.filter(x => x._2 >= threshold).map(x => x._1)
    for (i <- 3 to 10) {
      println(rs3.filter(x => x._2 >= i).map(x => x._1).count())
    }

    println("###" + stage.count() + "###")
    //stage.take(10).foreach(println)
    /*
    stage1.saveAsTextFile("/home/work/myt/stage1")
    stage2.saveAsTextFile("/home/work/myt/stage2")
    stage3.saveAsTextFile("/home/work/myt/stage3")
    */

    val conf = ConfigFactory.load()
    val sum = stage
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            uid =>
              val key = uid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(226)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    has = true
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n1 = n1 + 1
                }
                n = n + 1
                //redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((n, n1)).iterator
      }
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      println("###" + sum._1+ "###" + sum._2 )

  }

}
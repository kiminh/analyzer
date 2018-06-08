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
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

/**
  * Created by YuntaoMa on 06/06/2018.
  */

object TagBadUid {
  def main(args: Array[String]): Unit = {
    import com.redis.RedisClient
    val spark = SparkSession.builder()
      .appName("Tag bad uid")
      .enableHiveSupport()
      .getOrCreate()


    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    var stmt =
      """
        |SELECT searchid,uid,userid
        |FROM dl_cpc.cpc_union_log
        |WHERE `date` = "%s" AND isshow = 1
      """.stripMargin.format(date)

    var rs1 = spark.sql(stmt).rdd
      .map {
        row =>
          val searchid = row.getString(0)
          val uid = row.getString(1)
          val userid = row.getInt(2)
          (searchid, "user#" + userid + "u#" + uid)
      }

    stmt =
      """
        |SELECT searchid
        |FROM dl_cpc.cpc_union_trace_log
        |WHERE `date` = "%s" and trace_type like "active%%"
      """.stripMargin.format(date)

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

    val stage = rs3.filter(x => x._2 > 10).map(x => x._1)

    println("###" + stage.count() + "###")
    stage.take(10).foreach(println)
    /*
    stage1.saveAsTextFile("/home/work/myt/stage1")
    stage2.saveAsTextFile("/home/work/myt/stage2")
    stage3.saveAsTextFile("/home/work/myt/stage3")
    */

    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    stage.foreach {
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
              if (!has) {
                user.setInterestedWords(i, in)
                has = true
              } else {
                user.removeInterestedWords(i)
              }
            }
          }
          if (!has) {
            user.addInterestedWords(in)
          }
          redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
        }
    }
    

  }

}
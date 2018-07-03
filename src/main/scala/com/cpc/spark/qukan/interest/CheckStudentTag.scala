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
object CheckStudentTag {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val spark = SparkSession.builder()
      .appName("Tag user by zfb")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    println(date)
    var stmt =
      """
        |SELECT DISTINCT uid,interests
        |FROM dl_cpc.cpc_union_log
        |WHERE `date` = "%s" AND isshow = 1 AND (media_appsid = "80000001" OR media_appsid = "80000002")
        |LIMIT 10000000
      """.stripMargin.format(date)

    var rs1 = spark.sql(stmt).rdd
      .map {
        row =>
          val uid = row.getString(0)
          val interests = row.getString(1).split(",")
          var age_224 = false
          var age_225 = false
          interests.foreach {
            r =>
              if (r != ""){
                val tag = r.split("=")(0)
                if (tag.toInt == 224) {
                  age_224 = true
                } else if (tag.toInt == 225) {
                  age_225 = true
                }
              }
          }
          if (age_224 && age_225) {
            ("both_taged", 1)
          } else {
            ("both_taged", 0)
          }
      }
        .reduceByKey(_+_)
    rs1.toLocalIterator.foreach(println)

  }
}

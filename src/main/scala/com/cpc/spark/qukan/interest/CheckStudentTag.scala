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
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.cpc.spark.qukan.parser.HdfsParser

import scala.util.control._
object CheckStudentTag {
  def main(args: Array[String]): Unit = {
    val uid = args(0).split(",")
    val detail = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag user by zfb")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    uid.foreach {
      u =>
       var key = u + "_UPDATA"
        println(key)
       val buffer = redis.get[Array[Byte]](key).orNull
       if (buffer != null) {
         val user = UserProfile.parseFrom(buffer).toBuilder
         var t224 = false
         var t225 = false
         var t226 = false
         var t228 = false
         for (i <- 0 until user.getInterestedWordsCount) {
           val w = user.getInterestedWords(i)
           if (w.getTag == 224 && !t224) {
             println("student_tag")
             t224 = true
           }
           if (w.getTag == 225 && !t225) {
             println("not_student_tag")
             t225 = true
           }
           if (w.getTag == 226 && !t226) {
             println("active_tag")
             t226 = true
           }
           if (w.getTag == 228 && !t228) {
             t228 = true
             println("not new user")
           }
         }
         if (detail) {
           println(user)
         }
       }
    }
    val zfb = spark.read.parquet("/user/cpc/qtt-zfb/10").rdd
      .map {
        r =>
          val did = r.getAs[String]("did")
          val birth = r.getAs[String]("birth")
          val sex = r.getAs[String]("sex")
          (did, birth, sex)
      }.filter(x => x._1 == "864320038318439")
    println(zfb.count())
    zfb.toLocalIterator.foreach(println)
  }
}

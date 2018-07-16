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

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.cpc.spark.qukan.parser.HdfsParser
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.util.control._

object DailyReport {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val is_set = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Daily report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val stmt =
      """
        |select distinct device_code as did from dl_cpc.qukan_p_member_info where `day` = "%s" and create_time > "%s 00:00:00"
      """.stripMargin.format(date)

    println(stmt)

    val rs = spark.sql(stmt).rdd.map {
      r =>
        val did = r.getAs[String](0)
        (did)
    }
    println(rs.count())
    val conf = ConfigFactory.load()
    val sum = rs.repartition(500)
      .mapPartitions{
        p =>
          var young = 0
          var notYoung = 0
          val redis = new RedisClient(conf.getString("host"), conf.getInt("port"))
          p.foreach {
            r =>
              val key = r + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == 224) {
                    young += 1
                  } else if (w.getTag == 225) {
                    notYoung += 1
                  }
                }
              }
          }
          Seq((0, young), (1, notYoung)).iterator
      }
      .reduceByKey(_+_)
    sum.toLocalIterator.foreach(println)
  }

}

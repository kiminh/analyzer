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
      """.stripMargin.format(date, date)

    println(stmt)

    val rs = spark.sql(stmt).rdd.map {
      r =>
        val did = r.getAs[String](0)
        (did)
    }
    println(rs.count())
    val zfb = spark.read.parquet("user/cpc/qtt-zfb/10").rdd.map {
      r =>
        val did = r.getAs[String]("did")
        val birth = r.getAs[String]("birth")
        val age = 2018 - birth.toInt / 10000
        if (age < 22) {
          (did, 0)
        } else {
          (did, 1)
        }
    }
    val conf = ConfigFactory.load()
    val sum = rs.map(x => (x, 1)).leftOuterJoin(zfb).repartition(500)
      .mapPartitions{
        p =>
          var young = 0
          var notYoung = 0
          var zfb_num = 0
          var pre_num = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            r =>
              if (r._2._2 != None) {
                if (r._2._2 == 0) {
                  young += 1
                } else {
                  notYoung += 1
                }
                zfb_num += 1
              } else {
                pre_num += 1
                val key = r._1 + "_UPDATA"
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
          }
          Seq((0, young), (1, notYoung), (2, zfb_num), (3, pre_num)).iterator
      }
      .reduceByKey(_+_)
    sum.toLocalIterator.foreach(println)

//    val stmt2 =
//      """
//        |select uid, ext_int["lx_package"] from dl_cpc.cpc_union_log where `date` = "%s"
//      """.stripMargin.format(date)
//
//    val rs2 = spark.sql(stmt2).rdd.map {
//      r =>
//        val did = r.getAs[String](0)
//        val lx = r.getAs[Long](1)
//        (did, lx)
//    }.reduceByKey((x, y) => x)
//
//    println(rs.map(x => (x, 1)).join(rs2).filter(x => x._2._2 == 0).count())
  }

}

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


// 0 日新增学生用户预测
// 1 日新增非学生用户预测
// 2 日新增用户有支付宝数据
// 3 日新增用户无支付宝数据

object DailyReport {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val is_set = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Daily report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    student_app(spark, args)
    //checkUVTag(spark, args)
    //daily_cost(spark, args)



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

  def qukan_new_user(spark : SparkSession, args : Array[String]): Unit = {
    val days  = args(0).toInt
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
    val zfb = spark.read.parquet("qtt-zfb/10").rdd.map {
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
  }
  def checkUVTag(spark : SparkSession, args : Array[String]): Unit = {
    val days  = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val edate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val stmt =
      """
        |select distinct uid from dl_cpc.cpc_union_log where `date` = "%s"  and media_appsid  in ("80000001", "80000002") and adsrc = 1
      """.stripMargin.format(sdate)

    val rs = spark.sql(stmt).rdd.map {
      r =>
        val did = r.getAs[String](0)
        (did)
    }.distinct()

    import spark.implicits._
    println(rs.count())
    val zfb = spark.read.parquet("/user/cpc/qtt-zfb/10")
    rs.toDF("did").join(zfb, "did").rdd.map {
      r =>
        val birth = r.getAs[String]("birth")
        if (2018 - birth.toInt / 10000 < 22) {
          (0, 1)
        }  else {
          (1, 1)
        }
    }.reduceByKey(_+_)
      .toLocalIterator
      .foreach(println)
    val conf = ConfigFactory.load()
    val sum = rs.repartition(500)
      .mapPartitions{
        p =>
          var young = 0
          var notYoung = 0
          var active = 0
          var female = 0
          var male = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            r =>
                val key = r + "_UPDATA"
                val buffer = redis.get[Array[Byte]](key).orNull
                var is224 = false
                var is225 = false
                if (buffer != null) {
                  val user = UserProfile.parseFrom(buffer).toBuilder
                  if (user.getSex == 2) {
                    female += 1
                  } else if (user.getSex == 1) {
                    male += 1
                  }
                  for (i <- 0 until user.getInterestedWordsCount) {
                    val w = user.getInterestedWords(i)
                    if (w.getTag == 224) {
                      is224 = true
                    } else if (w.getTag == 225) {
                      is225 = true
                    } else if (w.getTag == 226) {
                      active += 1
                    }
                  }
                  if (is224) {
                    young += 1
                  }
                  if (is225) {
                    notYoung += 1
                  }
                }
          }
          Seq((0, young), (1, notYoung), (2,active), (3,female), (4,male)).iterator
      }
      .reduceByKey(_+_)
    sum.toLocalIterator.foreach(println)
  }
  def daily_cost(spark : SparkSession, args : Array[String]): Unit = {
    val days  = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val stmt =
      """
        |select bid, price, ext["cvr_threshold"]["int_value"], searchid, userid from  dl_cpc.cpc_union_log
        |where media_appsid in ("80000001","80000002") and `date` = "%s" and isclick = 1 and adsrc <= 1
      """.stripMargin.format(date)
    println(stmt)
    println("================================================")
    val res = spark.sql(stmt).rdd.map {
      r =>
        val bid = r.getAs[Int](0).toLong
        val price = r.getAs[Int](1).toLong
        val thresh = r.getAs[Int](2).toLong
        val sid = r.getAs[String](3)
        val userid = r.getAs[Int](4)
        (userid, (bid, price, thresh, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .sortBy(_._2._4, false)
      .take(100)

    res.foreach(println)
    res.map(x => (x._1, 1d * x._2._1 / x._2._4, x._2._2, 1d * x._2._3 / x._2._4))
      .foreach{
        x =>
          println("%s\t %8.2f\t %s\t %8.2f".format(x._1,x._2,x._3,x._4))
      }
  }
  def student_app(spark : SparkSession, args : Array[String]): Unit ={
    val sample = spark.read.parquet("/user/cpc/qtt-age-sample/p1").rdd.map{
      x =>
        if (x(2) != null) {
          (x.getAs[Int]("birth"), x.getAs[Seq[Row]]("apps").size, x.getAs[Seq[Row]]("apps"))
        } else {
          null
        }
    }.filter(_ != null)
    sample.flatMap{
      x =>
        x._3.map{
          r =>
            (r.getAs[String](0), 1)
        }
    }.reduceByKey(_+_).sortBy(_._2, false).toLocalIterator.foreach(println)

  }

}

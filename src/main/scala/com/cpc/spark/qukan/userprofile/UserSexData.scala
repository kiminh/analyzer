package com.cpc.spark.qukan.userprofile

import org.json4s
import scala.reflect.runtime.universe
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.optimization.{L1Updater, LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import java.io.File
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.HanLP
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import scala.util.hashing.MurmurHash3
import scala.util.control._

/**
 * 训练性别预测模型
用来跑数据
 */
object UserSexData {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataStart = args(0).toString()
    val dataEnd = args(1).toString()
    // 测试样例结果
    val dataout = args(2).toString()
//    val output2= args(3).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-DataCreateV2")
      .config("spark.driver.maxResultSize", "40G")
      .enableHiveSupport()
      .getOrCreate()

    import ctx.implicits._
    
    val HASHSUM = 100000
    // (deviceid,(titles,sex,titlesLike,readSec,likeSec))
    val qukanRead = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title, sec.second_level
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id 
        | LEFT JOIN algo_qukan.algo_feature_content_seclvl sec ON qc.id = sec.content_id
        |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        x =>
          (x.getString(0), x.getString(1), x.getString(2))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (x._2.map(_._2).toSeq, 0,Seq(""),x._2.map(_._3).toSeq,Seq("")))
      }
      .filter(_._2._1.length > 3)
      .repartition(200)
      
    val qukanLike = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title, qkc.content_id, sec.second_level
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
        |LEFT JOIN algo_qukan.algo_feature_content_seclvl sec ON qc.id = sec.content_id
        |WHERE qkc.cmd in (203,201,200) AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        x =>
          (x.getString(0), x.getString(1), x.getString(2))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (Seq(""),0,x._2.map(_._2).toSeq,Seq(""), x._2.map(_._3).toSeq ))
      }
      .filter(_._2._3.length > 1)
      .repartition(200)
 
    // (deviceid,(titles,sex,titlesLike,readSec,likeSec))
      // sex 1 man  2 woman
/*      val usersex = ctx.sql(
      "SELECT qpm.device_code,qpm.sex from gobblin.qukan_p_member_info qpm where qpm.day=\"2017-11-01\" AND qpm.update_time>=\"2017-05-01\" AND qpm.device_code is not null AND sex is not null"
          ).rdd
          .map { 
          x=>
            (x.getString(0),x.getLong(1))
        }.filter {
          x =>
            if (x._2 != 1 && x._2 != 2) {
              false
            }else {
              true
            }
        }.map {
          x =>
            (x._1,(Seq(""),x._2.toInt,Seq(""),Seq(""),Seq("")))
        }
        */
      val usersex = ctx.sql(
          """
          |SELECT DISTINCT info.device_code, zfb.info
          |FROM gobblin.qukan_member_zfb_log as zfb
          |INNER JOIN dl_cpc.qukan_p_member_info as info ON zfb.member_id = info.member_id
          |WHERE zfb.create_time >= '2018-01-01' and info.day = '2018-01-29'
          """.stripMargin
          ).rdd
          .map { 
        x=>
          val deviceid = x(0).toString()
          val sexarr = x.getString(1).split("gender\":\"")
          var sex = 0
          if (sexarr.length>1) {
             val sexc = sexarr(1).split("\"")(0) 
             if (sexc == "f" || sexc == "F") {
               sex = 2
             }
             if (sexc == "m" || sexc == "M"){
               sex = 1
             }
          }
          (deviceid, (Seq(""), sex, Seq(""),Seq(""), Seq("")))
      }.filter {
        x=>
          if (x._2._2 == 0){
            false
          }else {
            true
          }
      }.repartition(200)
      println("get usersex num [%d]".format(usersex.count()))

    // (titles,sex,titlesLike,readSec,likeSec)
    val allData = usersex
      .union(qukanRead)
      .reduceByKey {
        (x, y) =>
        var titles = x._1     
        var sex = x._2
        var readSec = x._4
        if (titles.length < 2) {
          titles = y._1
        }
        if (sex == 0) {
          sex = y._2
        }
        if (readSec.length < 2) {
          readSec = y._4
        }
        (titles, sex,Seq(""), readSec, Seq(""))
    }
    .union(qukanLike)
    .reduceByKey {
      (x,y) =>
        var titles = x._1
        var sex = x._2
        var titlesLike = x._3
        var readSec = x._4
        var likeSec = x._5
        if(titles.length < 2){
          titles = y._1
        }
        if (sex == 0){
          sex = y._2
        }
        if (titlesLike.length < 1) {
          titlesLike = y._3
        }
        if (readSec.length < 1) {
          readSec = y._4
        }
        if (likeSec.length < 1) {
          likeSec = y._5
        }
        (titles, sex, titlesLike,readSec, likeSec)
      
    }.filter {
        x =>
          (x._2._2 != 0) && ((x._2._1.length > 3) || (x._2._3.length > 1) || (x._2._4.length > 1))
      }.repartition(200)
    
    // (titles,sex,titlesLike,readSec,likeSec)
    allData.map {
      x =>
        "%d\t%s\t%s\t%s\t%s".format(x._2._2,x._2._1.mkString(","),x._2._3.mkString(","),x._2._4.mkString(","),x._2._5.mkString(","))
    }.saveAsTextFile(dataout)
    

    //统计分类准确率
    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}



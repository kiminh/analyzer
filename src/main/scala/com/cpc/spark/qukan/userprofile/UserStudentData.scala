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

object UserStudentData {
  
   def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataStart = args(0).toString()
    val dataEnd = args(1).toString()
    // 测试样例结果
    val dataout = args(2).toString()
//    val output2= args(3).toString()
    val ctx = SparkSession.builder()
      .appName("TrainStudentModel-DataCreateV2")
      .config("spark.driver.maxResultSize", "40G")
      .enableHiveSupport()
      .getOrCreate()

    import ctx.implicits._
    
    val HASHSUM = 100000
    // (deviceid,(isstudent,apps:string))
    val userapps = ctx.sql(
        """
        |SELECT distinct uid, pkgs FROM dl_cpc.cpc_user_installed_apps
        |WHERE load_date >= "%s" and load_date <= "%s"
        """.format(dataStart, dataEnd).stripMargin
        ).rdd
        .map {
      x=>
       val deviceid = x(0).toString()
       val apps = x(1).toString().replace("[", "").replace("]", "").toString()
       (deviceid,(2,apps))
    }.repartition(200)

      val userage = ctx.sql(
          """
          |SELECT DISTINCT info.device_code, zfb.info
          |FROM gobblin.qukan_member_zfb_log as zfb
          |INNER JOIN dl_cpc.qukan_p_member_info as info ON zfb.member_id = info.member_id
          |WHERE zfb.create_time >= '2018-01-01' and info.day = '%s'
          """.format(dataEnd).stripMargin
          ).rdd
          .map { 
        x=>
          val deviceid = x(0).toString()
          val agearr = x.getString(1).split("person_birthday\":\"")
          var isstudent = 0
          if (agearr.length>1) {
             val agec = agearr(1).split("\"")(0) 
             if (agec >= "19960101") {
                isstudent = 1 
             }
           }
          (deviceid, (isstudent,""))
      }.repartition(200)
      println("get usersex num [%d]".format(userage.count()))

    // (titles,sex,titlesLike,readSec,likeSec)
    val allData = userage
      .union(userapps)
      .reduceByKey {
        (x, y) =>
        var apps = x._2
        var isstudent = x._1
        if (apps == "") {
          apps = y._2
        }
        if (isstudent == 2) {
          isstudent = y._1
        }
        (isstudent, apps)
    }
    .filter {
        x =>
          if (x._2._1 == 2 || x._2._2 == "") {
            false
          } else {
            true
          }
      }.repartition(200)
    
    // (isstudent, apps)
    allData.map {
      x =>
        "%d\t%s".format(x._2._1,x._2._2)
    }.saveAsTextFile(dataout)
    

    //统计分类准确率
    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}
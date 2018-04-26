package com.cpc.spark.qukan.userprofile

import scala.reflect.runtime.universe
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
import org.apache.spark.mllib.classification.LogisticRegressionModel
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import userprofile.Userprofile.UserProfile

/**
  * 训练性别预测模型
  */
object TrainSex2{

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataStart = args(0).toString()
    val dataEnd = args(1).toString()
    val modelin = args(2).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-NEW2")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    
        val HASHSUM = 100000
    // (deviceid,(titles,sex,titlesLike))
    val qukanRead = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
        |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        x =>
          (x.getString(0), x.getString(1))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (x._2.map(_._2).toSeq, 0,Seq("")))
      }
      .filter(_._2._1.length > 3)
      
    val qukanLike = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
        |WHERE qkc.cmd in (203,201,200) AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        x =>
          (x.getString(0), x.getString(1))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (Seq(""),0,x._2.map(_._2).toSeq))
      }
      .filter(_._2._3.length > 1)
 
      // sex 1 man  2 woman
      val usersex = ctx.sql(
      "SELECT qpm.device_code,qpm.sex from gobblin.qukan_p_member_info qpm where qpm.day=\"2017-09-01\" AND qpm.update_time>=\"2017-05-01\" AND qpm.device_code is not null AND sex is not null"
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
            (x._1,(Seq(""),x._2.toInt,Seq("")))
        }
        
    val adplan = ctx.sql(
    """
    |select uid, planid 
    | from dl_cpc.cpc_union_log 
    | where date >="%s" and date <= "%s" and isclick = 1 and planid > 1000000
    |""".stripMargin.format(dataStart, dataEnd)
    ).rdd
    .map {
      x =>
        (x.getString(0), x.getInt(1))
    }
    .groupBy(_._1)
    .map {
      x =>
        (x._1, (Seq(""),0,x._2.map(_._2).toSeq))
    }

    val allData = usersex
      .union(qukanRead)
      .reduceByKey {
        (x, y) =>
        var titles = x._1     
        var sex = x._2
        if (titles.length < 2) {
          titles = y._1
        }
        if (sex == 0) {
          sex = y._2
        }
        (titles, sex,Seq(""))
    }
    .union(qukanLike)
    .reduceByKey {
      (x,y) =>
        var titles = x._1
        var sex = x._2
        var titlesLike = x._3
        if(titles.length < 2){
          titles = y._1
        }
        if (sex == 0){
          sex = y._2
        }
        if (titlesLike.length < 1) {
          titlesLike = y._3
        }
        (titles, sex, titlesLike)
      
    }
      .filter {
        x =>
          (x._2._2 != 0) && ((x._2._1.length > 3) || (x._2._3.length > 1))
      }
      .map {
      x => 
        val devid : String = x._1
        val title : String = x._2._1.mkString("")
        val sex : Int = x._2._2
        val titlesLike = x._2._3.mkString("")
        val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
        val termarr = terms.split(" ")
        var done : Boolean = false
        var els = Seq[(Int,Double)]()
        for (i <- 0 to  termarr.length - 1) {
          val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } 
        }
        // 用户like
        val termsLike = HanLP.segment(titlesLike).filter{x => x.length() > 1}.map{x => x.word}.mkString(" ")
        val termarrLike = termsLike.split(" ")
        for(i<- 0 to termarrLike.length-1) {
          val tag = (MurmurHash3.stringHash(termarrLike(i))%HASHSUM + HASHSUM)%HASHSUM + HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } 
        }
       (sex,Vectors.sparse(HASHSUM + HASHSUM, els))
    }.map{
      x =>
        LabeledPoint(x._1-1, x._2)
    }
    .repartition(200)

    val lrmodel = LogisticRegressionModel.load(ctx.sparkContext, modelin)
    lrmodel.clearThreshold()

    val testpredictionAndLabel2 = allData.map { x =>
      (lrmodel.predict(x.features), x.label.toDouble)
    }
    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel2)
    val testpredictionAndLabel = testpredictionAndLabel2.map {
      x=>
        var idx = (x._1 * 10).toInt
        if (x._2.toInt == 0) {
          (idx,(1,1,0))
        } else {
          (idx,(1,0,1))
        }
      }
    println("ans start =========")
    testpredictionAndLabel.reduceByKey {
       (x ,y) =>
         (x._1+y._1,x._2+y._2,x._3+y._3)
    }
    .collect().foreach(println)
    println("ans end =========")
    println("训练结果")
    println("auROC:",metrics.areaUnderROC())
    println("auPRC:", metrics.areaUnderPR())


    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}

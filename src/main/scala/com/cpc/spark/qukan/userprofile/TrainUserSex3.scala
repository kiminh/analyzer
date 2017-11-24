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
用于预测数据
  */
object TrainSex3{

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataStart = args(0).toString()
    val dataEnd = args(1).toString()
    val modelin = args(2).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-%s-->%s".format(dataStart,dataEnd))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._


    val conf = ConfigFactory.load()
        val HASHSUM = 100000
    // (deviceid,(titles,titlesLike,iscpc))
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
          (x._1, (x._2.map(_._2).toSeq, Seq(""),0))
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
          (x._1, (Seq(""),x._2.map(_._2).toSeq,0))
      }
      .filter(_._2._2.length > 1)
 
      // sex 1 man  2 woman
      val usersex = ctx.sql(
//      "SELECT qpm.device_code,qpm.sex from gobblin.qukan_p_member_info qpm where qpm.day=\"2017-09-01\" AND qpm.update_time>=\"2017-05-01\" AND qpm.device_code is not null AND sex is not null"
          """
          SELECT distinct uid from dl_cpc.cpc_union_log where `date` = "%s" and sex = 0
          """.format(dataEnd)
          ).rdd
          .map { 
          x=>
            (x.getString(0))
        }.map {
          x =>
            (x,(Seq(""),Seq(""),1))
        }
        
    val allData = usersex
      .union(qukanRead)
      .reduceByKey {
        (x, y) =>
        var titles = x._1
        var iscpc = x._3
        if (titles.length < 2) {
          titles = y._1
        }
        if (iscpc == 0) {
          iscpc = y._3
        }
        (titles, Seq(""),iscpc)
    }.filter(_._2._3 == 1) 
    .union(qukanLike)
    .reduceByKey {
      (x,y) =>
        var titles = x._1
        var titlesLike = x._2
        var iscpc = x._3
        if(titles.length < 2){
          titles = y._1
        }
        if (iscpc == 0){
          iscpc = y._3
        }
        if (titlesLike.length < 1) {
          titlesLike = y._2
        }
        (titles, titlesLike, iscpc)
      
    }
      .filter {
        x =>
          (x._2._3 == 1) && ((x._2._1.length > 3) || (x._2._2.length > 1))
      }
      .repartition(200)
      .map{
      x => 
        val title : String = x._2._1.mkString("")
        val devid : String = x._1
        val titlesLike : String = x._2._2.mkString("")
        val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
        val termarr = terms.split(" ")
        var done : Boolean = false
        var els = Seq[(Int,Double)]()
        for (i <- 0 to  termarr.length - 1) {
          var insert = false
          val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
           insert = true
          } 
        }
        
        // 用户like
        val termsLike = HanLP.segment(titlesLike).filter{x => x.length() > 1}.map{x => x.word}.mkString(" ")
        val termarrLike = termsLike.split(" ")
        for(i<- 0 to termarrLike.length-1) {
          var insert = false
          val tag = (MurmurHash3.stringHash(termarrLike(i))%HASHSUM + HASHSUM)%HASHSUM + HASHSUM 
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
           insert = true
          } 
        }
       (devid,Vectors.sparse(HASHSUM*2  , els))
      }
    
    val lrmodel = LogisticRegressionModel.load(ctx.sparkContext, modelin)
    lrmodel.clearThreshold()

    val ansret = allData.map {
      x =>
        val predata = lrmodel.predict(x._2)
        if (predata > 0.63 ) {
          (x._1, 2)
        } else if (predata < 0.33) {
          (x._1, 1)
        } else {
          (x._1, 0)
        }
    }

    val sum = ansret.mapPartitions {
      part =>
        var n1 = 0  // all
        var n2 = 0  // update
        var n3 = 0  // male
        var n4 = 0  // female
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        part.foreach {
            x =>
              var deviceid: String = x._1
              var user: UserProfile.Builder = null
              val key = deviceid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
          		n1 = n1 + 1
              if (buffer == null) {
                user = UserProfile.newBuilder().setDevid(deviceid)
              } else {
                try {
                  user = UserProfile.parseFrom(buffer).toBuilder
                } finally {
                  user = UserProfile.newBuilder().setDevid(deviceid)
                }
                n2 = n2 + 1
              }
              if (x._2 == 1 || x._2 == 2) {
                user.setSex(x._2)
                if (x._2 == 1){
                  n3 = n3 + 1
                }
                if (x._2 == 2) {
                  n4 = n4 + 1
                }

                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
        }
        Seq((0, n1), (1, n2),(2,n3),(3,n4)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    var n4 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(5)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } 
          if (x._1 == 1){
            n2 = x._2
          }
          if (x._1 == 2) {
            n3 = x._2
          }
          if (x._1 == 3) {
            n4 = x._2
          }
      }
    println("\nDATE: %s total: %d updated: %d   male: %d  female:%d \n".format(dataEnd, n1, n2, n3, n4))

    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}

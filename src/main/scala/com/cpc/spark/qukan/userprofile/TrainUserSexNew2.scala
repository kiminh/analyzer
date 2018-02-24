package com.cpc.spark.qukan.userprofile

import scala.reflect.runtime.universe

import scala.collection.mutable.ArrayBuffer
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
用于计算模型
 */
object TrainUserSexNew2 {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val appname = args(0).toString()
    val input = args(1).toString()
    // 测试样例结果
    val modelout = args(2).toString()
    val ctx = SparkSession.builder()
      .appName(appname)
      .config("spark.driver.maxResultSize", "40G")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    


    val HASHSUM = 100000
    // (sex, readtitles,titlesLike, readSec, likeSec))
    val allDataRead = ctx.sparkContext.textFile(input).map {
      x => 
         val arr = x.split("\t")
         if(arr.length == 5) {
           var sex = -1
           if (arr(0) == "1") {
             sex = 1 
           } else if (arr(0) =="2" ){
             sex = 2
           }
           (sex, arr(1), arr(2),arr(3),arr(4))
         }
         else if (arr.length == 4) {
          var sex = -1
          if (arr(0) == "1") {
             sex = 1 
          } else if (arr(0) =="2" ){
            sex = 2
          }
          (sex, arr(1), arr(2), arr(3),"")
        } else if (arr.length == 3){
          var sex = -1
          if (arr(0) == "1") {
             sex = 1 
          } else if (arr(0) =="2" ){
            sex = 2
          }
          (sex, arr(1), arr(2),"","")
        } else if (arr.length == 2) {
          var sex = -1
          if (arr(0) == "1") {
             sex = 1 
          } else if (arr(0) =="2" ){
            sex = 2
          }
          (sex, arr(1), "","","")
        }else {
          (-1, "","","","")
        }
    }.filter(_._1 != -1)
    .map {
      x =>
        (x._1,x._2,x._3,x._4,x._5)
    }
    .repartition(400)
    
  
    
    val allData = allDataRead
    .map {
      x => 
        val title : String = x._2
        val sex : Int = x._1
        val titlesLike = x._3
        val readSec : String = x._4
        var likeSec : String = x._5

        var els = Seq[(Int,Double)]()
        // 用户read
/*        val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
        val termarr = terms.split(" ")
        var done : Boolean = false
        for (i <- 0 to  termarr.length - 1) {
          var insert = false
          val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
           insert = true
          } 
        }
        * 
        */
        
        // 用户like
        /*
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
        * 
        */
        
        //阅读二级分类
        var readSecArr = readSec.split(",")
        for(i<- 0 to readSecArr.length - 1) {
         val tag = (MurmurHash3.stringHash(readSecArr(i))%HASHSUM + HASHSUM) % HASHSUM + 0*HASHSUM 
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } 
        }
        
        //like二级分类
        var likeSecArr = likeSec.split(",")
        for(i<- 0 to likeSecArr.length - 1) {
         val tag = (MurmurHash3.stringHash(likeSecArr(i))%HASHSUM + HASHSUM) % HASHSUM + 1*HASHSUM 
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } 
        }
        
 
       (sex,Vectors.sparse(HASHSUM*2  , els))
    }.map{
      x =>
        LabeledPoint(x._1-1, x._2)
    }
    .randomSplit(Array(0.9,0.1))

    val trainDataRdd = allData(0)
    val testDataRdd = allData(1)

    val traincount = trainDataRdd.count()
    println("训练集数据量:")
    println(traincount)

    //训练模型
    val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs.optimizer.setUpdater(new L1Updater())
  //  lbfgs.optimizer.setNumIterations(20)
    lbfgs.optimizer.setConvergenceTol(0.0001)
    val lrmodel = lbfgs.run(trainDataRdd)
//    lrmodel.setThreshold(0.5)
    lrmodel.clearThreshold()
    lrmodel.save(ctx.sparkContext, modelout)

    println("测试集数据量:")
    println(testDataRdd.count())

    val testpredictionAndLabel2 = testDataRdd.map { x =>
      (lrmodel.predict(x.features), x.label.toDouble)
    }
    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel2)
    val testpredictionAndLabel = testpredictionAndLabel2.map {
      x=>
        var idx = (x._1 * 100).toInt
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
    .collect().foreach{
      x =>
      print("%d\t%d\t%d\t%d\t\n".format(x._1,x._2._1,x._2._2,x._2._3))
    }
    println("ans end =========")
    println("训练结果")
    println("auROC:",metrics.areaUnderROC())
    println("auPRC:", metrics.areaUnderPR())

    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}



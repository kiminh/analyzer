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
import scala.collection.mutable.ArrayBuffer
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

import com.google.gson.JsonArray
import com.google.gson.JsonIOException
import com.google.gson.JsonObject
import com.google.gson.JsonParser

object TrainAgeNew {
  
  def checkValid(check:Int, age:Int, sex:Int) : (Boolean) = {
      if ( check == 1 ) {
         if (sex != 1 || (age != 1 && age != 2 && age != 3)){
             false
         } else {
             true
         }
      }
      else if ( check == 2 ) {
         if (sex != 2 || (age != 1 && age != 2 && age != 3)){
             false
         } else {
             true
         }
      }
      else if ( check == 3 ) {
         if (sex != 1 || (age != 4 && age != 5 && age != 6)){
             false
         } else {
             true
         }
      }
      else if ( check == 4 ) {
         if (sex != 2 || (age != 4 && age != 5 && age != 6)){
             false
         } else {
             true
         }
      }
      else if ( check == 5 && sex != 2) {
        false
      } else {
        true
      }
  }
  
  def main(args: Array[String]) : Unit = {
       val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
//    val input = args(0).toString()
    val output = args(0).toString()
    val dataStart = args(1).toString()
    val dataEnd= args(2).toString()
    val modelout = args(3).toString()
    val ctx = SparkSession.builder()
      .appName("TrainAgeModel-NEW2-HXJ")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val HASHSUM = 150000
    
    var requser = ctx.sql(
         """
        |select qpm.member_id,qpm.device_code,qpm.birth
        |from gobblin.qukan_p_member_info qpm
        |where qpm.day="2017-09-01" AND qpm.update_time>="2017-05-01" AND qpm.device_code is not null 
        |AND birth is not null
      """.stripMargin)
      .map{
         x =>
           var member_id = x(0).toString()
           var deviceid = x(1).toString()
           var birth = x(2).toString()
           var age= 0
           if (birth >= "1990-01-01") {
              age= 0 
           } else if (birth >= "1975-01-01") {
             age= 1
           } else {
             age= 2
           }
           (member_id,(age,"",deviceid))
       }.rdd
   
       println("train num is :" + requser.count())
      
      val readdata = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title,member_id
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
        |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd)).rdd
        .map {
        x =>
          (x.getInt(2).toString(), x.getString(1), x.getString(0))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (-1, x._2.map(_._2).toSeq, x._2.head._3))
      }
      .filter(_._2._2.length > 3)
      .map{
        x =>
          (x._1,(-1, x._2._2.mkString("$|$"), x._2._3))
      }
      println("readdata num: " + readdata.count())
      
          //获取用户标题
    val readTitleData = readdata.map {
      x =>
        val titles = x._2._2
        HanLP.segment(titles).filter { x => x.nature.toString().startsWith("n") }.map { x => x.word }.mkString(" ").split(" ")
    }
    .flatMap {
        x =>
          val ans = new ArrayBuffer[String]()
          x.map(x => ans += x)
          ans
      }
      .map {
        x =>
          (x, (1))
      }
      .reduceByKey {
        (a, b) =>
          (a + b)
      }
      .sortBy(_._2, false)
      .take(HASHSUM)

      var keyMaps: Map[String, Int] = Map()
      var keyMapsNum = 0
      readTitleData.foreach {
        x =>
          keyMaps+=(x._1->keyMapsNum)

          keyMapsNum += 1
      }

      val allData = requser.union(readdata).reduceByKey {
        (x,y) =>
          var age = x._1
          var titles =  x._2
          var deviceid = x._3
          if (age == -1) {
            age = y._1
          }
          if (titles.length() < 3) {
            titles = y._2
          }
          if (deviceid.length < 5) {
            deviceid = y._3
          }
          (age, titles, deviceid)
      }.filter{
        x =>
          if (x._2._1 == -1) {
            false
          } else if (x._2._3.length() < 5) {
            false
          } else {
            true
          }
      }
      .map {
        x =>
          val titles = x._2._2
          val age = x._2._1
        val terms = HanLP.segment(titles).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
        val termarr = terms.split(" ")
        var done : Boolean = false
        var els = Seq[(Int,Double)]()
        for (i <- 0 to  termarr.length - 1) {
/*          val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } 
          * 
          */
          val tmpVal = keyMaps.get(termarr(i))
          if ((tmpVal != None) && (tmpVal.head >0)) {
            val tag = tmpVal.head
            if(els.exists(x=>(x._1 == tag)) == false) {
              els = els :+ (tag, 1D)
            } else {
              for(cnt <- 4 to 1) {
                var cntidx = els.indexOf((tag, cnt.toDouble))
                if (cntidx != -1) {
                  els.set(cntidx,(tag , (cnt+1).toDouble))
                }
              }
            }
          }
        }
        (age,Vectors.sparse(HASHSUM, els))

          //(x._2._3, x._2._2, x._2._1)
      }
      .randomSplit(Array(0.9, 0.1))



      
      val traindata = allData(0)
      val testdata = allData(1)
       println("样本总数:" + traindata.count() )
       println("测试总数:" + testdata.count() )
/*      val trainDataRdd = traindata.map{
        x =>
          LabeledPoint(x._1, x._2)
     }
    println("训练集数据量:" + trainDataRdd.count())
    * 
    */
    
    val traindataRdd0 = traindata.map {
       x => 
         if (x._1 == 0 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()

    val traindataRdd1 = traindata.map {
       x => 
         if (x._1 == 1 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()

    val traindataRdd2 = traindata.map {
       x => 
         if (x._1 == 2 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()


    // 训练0类型数据
    val lbfgs0 = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs0.optimizer.setUpdater(new L1Updater())
    lbfgs0.optimizer.setConvergenceTol(1e-6)
    val lrmodel0 = lbfgs0.run(traindataRdd0)
    lrmodel0.clearThreshold()
    lrmodel0.save(ctx.sparkContext, modelout+"_0")

    // 训练1类型数据
    val lbfgs1 = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs1.optimizer.setUpdater(new L1Updater())
    lbfgs1.optimizer.setConvergenceTol(1e-6)
    val lrmodel1 = lbfgs1.run(traindataRdd1)
    lrmodel1.clearThreshold()
    lrmodel1.save(ctx.sparkContext, modelout+"_1")

    // 训练2类型数据
    val lbfgs2 = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs2.optimizer.setUpdater(new L1Updater())
    lbfgs2.optimizer.setConvergenceTol(1e-6)
    val lrmodel2 = lbfgs2.run(traindataRdd2)
    lrmodel2.clearThreshold()
    lrmodel2.save(ctx.sparkContext, modelout+"_2")

    val testDataRdd0 = testdata.map{
        x =>
         if (x._1 == 0 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()

    val testDataRdd1 = testdata.map{
        x =>
         if (x._1 == 1 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()

    val testDataRdd2 = testdata.map{
        x =>
         if (x._1 == 2 ){
           LabeledPoint(1, x._2)
         } else {
           LabeledPoint(0,x._2)
         }
    }.cache()


//    testDataRdd0.map { x =>
    traindataRdd0.map { x =>
      (lrmodel0.predict(x.features), x.label.toDouble)
    }.map{
      x =>
        val level = (x._1 * 10).toInt
        if (x._2 ==1 ){
          (level, (1,0))
        } else {
          (level, (0,1))
        } 
      }.reduceByKey {
      (x,y) =>
        (x._1+y._1, x._2+y._2)
    }.saveAsTextFile(output+"_0")

    testDataRdd1.map { x =>
      (lrmodel1.predict(x.features), x.label.toDouble)
    }.map{
      x =>
        val level = (x._1 * 10).toInt
        if (x._2 ==1 ){
          (level, (1,0))
        } else {
          (level, (0,1))
        } 
      }.reduceByKey {
      (x,y) =>
        (x._1+y._1, x._2+y._2)
    }.saveAsTextFile(output+"_1")

//    testDataRdd2.map { x =>
    testDataRdd2.map { x =>
      (lrmodel2.predict(x.features), x.label.toDouble)
    }.map{
      x =>
        val level = (x._1 * 10).toInt
        if (x._2 ==1 ){
          (level, (1,0))
        } else {
          (level, (0,1))
        } 
      }.reduceByKey {
      (x,y) =>
        (x._1+y._1, x._2+y._2)
    }.saveAsTextFile(output+"_2")



    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s"+"   ------> END")
    /*
    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel)
    println("auc:"+metrics.areaUnderROC())

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("正确率：")
    println(testaccuracy)
    * 
    */

  }

    
}
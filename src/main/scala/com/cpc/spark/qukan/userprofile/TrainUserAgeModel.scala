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

object TrainUserAgeModel {
  
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

    val HASHSUM = 20000
    
    //(member_id,(age,title,deviceid))
    val requser = ctx.sql("select member_id, question, answer from gobblin.qukan_ask_user_statistics_2").map {
         x =>
           var member_id = x(0).toString()
           var question = x(1).toString()
           var answer = x(2).toString()
           var sex = 0
           var age = 0
           var check = 0
           var valid = true
           if (question == "q_1") {
             sex = answer.toInt
           } else if (question == "q_2") {
             age = answer.toInt
           } else if (question == "q_3") {
             check = answer.toInt
           } else {
             valid = false
           }
         (member_id, (valid, sex,age,check))
       }.filter{
         x=>
           x._2._1
       }
       .rdd.reduceByKey{
        (x,y) =>
          var sex = x._2
          var age = x._3
          var check = x._4
          if (sex == 0) {
            sex = y._2
          }
          if (age == 0) {
            age = y._3
          }
          if (check == 0) {
            check = y._4
          }
          (x._1,sex,age,check)
      }.filter {
         x =>
           val check = x._2._4
           val sex = x._2._2
           val age = x._2._3
           if (x._1 == false) {
             false
           } else {
             checkValid(check, age, sex)
           }
       }.map{
         x=>
           // 0: <18  1:18-23   2:24-27  3:28-34 4:35-40 5:40-55 6: >55
           // real_age 0 0-23  1 24-40  2 41-100
           var realage = 0
           var age = x._2._3
           if (age == 3 || age == 4 || age == 5) {
              realage = 1 
           } else if (age > 5) {
             realage = 2
           }
           (x._1,(realage,"",""))
      }
       
       println("train num is :" + requser.count())

       val plandata = ctx.sql("select uid, ")
       /*
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



    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")
    /*
    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel)
    println("auc:"+metrics.areaUnderROC())

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("正确率：")
    println(testaccuracy)
    * 
    */
*/
  }

    
}
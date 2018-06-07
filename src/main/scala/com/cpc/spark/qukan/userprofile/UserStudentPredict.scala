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
import org.apache.spark.mllib.classification.LogisticRegressionModel
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import userprofile.Userprofile.UserProfile
import util.control.Breaks._


object UserStudentPredict 
{
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dateNow = args(0).toString()
    val modelin = args(1).toString()
    val ctx = SparkSession.builder()
    .appName("UserStudent Predict date[%s]".format(dateNow))
    .config("spark.driver.maxResultSize", "40G")
    .enableHiveSupport()
    .getOrCreate()
    
    
    val conf = ConfigFactory.load()

    import ctx.implicits._
    
    val HASHSUM = 100000
    // FOR  TEST    limit 100
    val sqldata = "SELECT distinct uid, pkgs FROM dl_cpc.cpc_user_installed_apps WHERE load_date = '%s' ".format(dateNow).stripMargin
    println("sqldata[%s]".format(sqldata))
    val userapps = ctx.sql(sqldata).rdd.map 
    {
      x=>
      val deviceid = x(0).toString()
      val apps = x(1).toString().replace("[", "").replace("]", "").toString()
      (deviceid,(apps))
    }
    .filter
    {
      x=>
      if( x._2.length() < 30){
        false
        }else {
          true
        }
      }
      .repartition(200)
      
      println("userapps num is %d".format(userapps.count()))


      val pdata = userapps.map {
        x => 
        val apps: String = x._2
        val devid: String = x._1
        var els = Seq[(Int,Double)]()
        //app hash
        var readSecArr = apps.split(",")
        for(i<- 0 to readSecArr.length - 1) {
         val tag = (MurmurHash3.stringHash(readSecArr(i).replace(" ", ""))%HASHSUM + HASHSUM) % HASHSUM 
         if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
         } 
       }
       (devid,Vectors.sparse(HASHSUM  , els))
     }


     val lrmodel = LogisticRegressionModel.load(ctx.sparkContext, modelin)
     lrmodel.clearThreshold()
     
     val allans = pdata.map{
      x=>
      val predata = lrmodel.predict(x._2)
      var isstudent = 2
      if(predata <= 0.3) {
        isstudent = 0 
      }
      if(predata >= 0.7) {
        isstudent = 1
      }
      (x._1, (isstudent))
    }.repartition(100)
    
    println("allans num is %d".format(allans.count()))
    
    //FOR TEST
//    allans.collect().foreach{
//      x=>
//        println("%s\t%d".format(x._1,x._2))
//    }

    val sum = allans.mapPartitions {
      part =>
        var n1 = 0  // all
        var n2 = 0  // update
        var n3 = 0  // student
        var n4 = 0  // not student
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
          }
          else 
          {
            try {
              user = UserProfile.parseFrom(buffer).toBuilder
              } catch {
                case ex: Exception=>{
                user = UserProfile.newBuilder().setDevid(deviceid)
                }
              }
              n2 = n2 + 1
            }
            val oldinterests = user.getInterestsList()
            var needupdate = false
            if (x._2 == 1) {
              needupdate = true
            // 小于22岁，学生人群
            breakable(
              for (i <- 0 until oldinterests.length) {
                var tmp = oldinterests.get(i)
                if (tmp.getTag() == 224 || tmp.getTag() == 225) {
                 needupdate = false
                 break()
               }
             }
           )
          }
          if (x._2 == 0){
            needupdate = true
            //大于22岁，非学生人群
            breakable(
              for(i<-0 until oldinterests.length) {
                var tmp = oldinterests.get(i)
                if(tmp.getTag() == 225 || tmp.getTag() == 224) {
                  needupdate = false
                  break()
                }
              }
              )
          }
          if(needupdate == true) {
            var interest = userprofile.Userprofile.InterestItem.newBuilder()
            interest.setScore(100)
            if(x._2 == 1){
              interest.setTag(224)
              n3 = n3+1
            }
            if(x._2 == 0) {
              interest.setTag(225)
              n4 = n4+1
            }
            user.addInterestedWords(interest)
            redis.setex(key, 3600*24*7, user.build().toByteArray())
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
    println("\nDATE: %s total: %d updated: %d   student: %d  notstudent:%d \n".format(dateNow, n1, n2, n3, n4))

    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")
  }

}
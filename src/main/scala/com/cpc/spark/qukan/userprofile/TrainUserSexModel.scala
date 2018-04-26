package com.cpc.spark.qukan.userprofile

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
object TrainSexModel {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataStart = args(0).toString()
    val dataEnd = args(1).toString()
    // 测试样例结果
    val dataout = args(2).toString()
//    val output2= args(3).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-DataCreate")
      .config("spark.driver.maxResultSize", "40G")
      .enableHiveSupport()
      .getOrCreate()

    import ctx.implicits._
    
    // (deviceid,(titles,sex,titlesLike,otherAds))
/*    val adsOther = ctx.sparkContext.textFile("/user/cpc/hxj/ads_other/")
    val adsOtherdata = adsOther
    .map {
      x =>
        var arr = x.split("\t")
        if (arr.length != 2) {
          ("","")
        } else {
           (arr(0).toString(),arr(1).toString())
        }
    }.groupBy(_._1)    
    .map {
      x =>
        (x._1,(Seq(""),0,Seq(""),x._2.map(_._2).toSeq))
    }
    .filter{
      x =>
        if (x._1.length() < 2) {
          false
        } else {
          true
        }
    }
    * *
    */
    val adsOtherdata = ctx.sql ("select field['deviceCode'].string_type, field['title'].string_type,field['desc'].string_type from dl_lechuan.qukan_report_log_p where day >= '%s' and day <= '%s' and field['title'].string_type > '' and cmd in (9001,9002) and field['action'].string_type = '5'".format(dataStart,dataEnd))
    .rdd
    .map { 
      x =>
        (x.getString(0), x.getString(1)+x.getString(2))
      }
    .groupBy(_._1)
    .map {
      x => 
        (x._1, (Seq(""),0, Seq(""), x._2.map(_._2).toSeq))
    }
    .filter {
      x =>
        if (x._1 == null) {
          false
        } else if (x._1.length() < 2 ) {
          false
        } else {
          true
        }
    }
    
    println("adsOtherData count: %d".format(adsOtherdata.count()))

    val HASHSUM = 100000
    // (deviceid,(titles,sex,titlesLike,adsother))
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
          (x._1, (x._2.map(_._2).toSeq, 0,Seq(""),Seq("")))
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
          (x._1, (Seq(""),0,x._2.map(_._2).toSeq,Seq("")))
      }
      .filter(_._2._3.length > 1)
 
      // sex 1 man  2 woman
      val usersex = ctx.sql(
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
            (x._1,(Seq(""),x._2.toInt,Seq(""),Seq("")))
        }
        
/*    val adplan = ctx.sql(
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
        (x._1, (Seq(""),0,x._2.map(_._2).toSeq,Seq("")))
    }
*/
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
        (titles, sex,Seq(""),Seq(""))
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
        (titles, sex, titlesLike,Seq(""))
      
    }.union(adsOtherdata)
    .reduceByKey {
      (x,y) =>
        var titles = x._1
        var sex = x._2
        var titlesLike = x._3
        var adsother =x._4
        if (sex == 0) {
          sex = y._2
        }
        if (titles.length < 1) {
          titles = y._1
        }
        if (titlesLike.length < 1) {
          titlesLike = y._3
        }
        if (adsother.length < 1) {
          adsother = y._4
        }
        (titles, sex, titlesLike, adsother)
    }
    .filter {
        x =>
          (x._2._2 != 0) && ((x._2._1.length > 3) || (x._2._3.length > 1) || (x._2._4.length > 1))
      }
    
    allData.map {
      x =>
       var titles =  x._2._1
       var titlesLike = x._2._3
       var adsother = x._2._4
       if (adsother.length > 0) {
         (3,(1))
       } else if (titlesLike.length > 1){
         (2,(1))
       } else if (titles.length > 1 ) {
         (1,(1))
       } else {
         (0,(1))
       }
    }.reduceByKey {
      (x,y) =>
        x+y
    }.collect()
    .foreach {
      x =>
        print("%d\t%d\n".format(x._1,x._2))
    }

    allData.map {
      x =>
        "%d\t%s\t%s\t%s".format(x._2._2,x._2._1.mkString(","),x._2._3.mkString(","),x._2._4.mkString(","))
    }.saveAsTextFile(dataout)
    
      /*
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
  //  lbfgs.optimizer.setConvergenceTol(0.001)
    val lrmodel = lbfgs.run(trainDataRdd)
//    lrmodel.setThreshold(0.5)
    lrmodel.clearThreshold()
    lrmodel.save(ctx.sparkContext, modelout)

//    val model = LogisticRegressionWithSGD.train(trainDataRdd.rdd, 50)

    //测试数据集，做同样的特征表示及格式转换
    /*
    var testwordsData = tokenizer.transform(test)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    * 
    */
//    var testrescaledData = idfModel.transform(testfeaturizedData)

    println("测试集数据量:")
    println(testDataRdd.count())


    val testpredictionAndLabel2 = testDataRdd.map { x =>
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
//    .saveAsTextFile(output2)
/*      .foreach {
        x =>
          println("idx:"+x._1 + "\tpre:"+x._2._1+"\t0:"+x._2._2+"\t1:"+ x._2._3)
      }
*/
//      println("结果集数据量:" + testpredictionAndLabel.count())
      println("训练结果")
      println("auROC:",metrics.areaUnderROC())
      println("auPRC:", metrics.areaUnderPR())
/*      println("precisionByThreshold:")
      metrics.precisionByThreshold().collect().foreach(println)
      println("recallByThreshold:")
      metrics.recallByThreshold().collect().foreach(println)
      println("f1ByThreshold:")
      metrics.fMeasureByThreshold().collect().foreach(println)
      * 
      */

//    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel)
//    println("auc:"+metrics.areaUnderROC())
*/
    //统计分类准确率
    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}



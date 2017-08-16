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

/**
 * 训练性别预测模型
 */
object TrainSexModel {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val input = args(0).toString()
    val output = args(1).toString()
    val output2 = args(2).toString()
    val modelout = args(3).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-NEW2")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    /*
    val log = ctx.sql("select sex,title from test.device_view_info where title is not null limit 10000").map { 
      case Row(sex: Long, title: String) =>
        val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
        Title(sex,terms)
     }.randomSplit(Array(0.9, 0.1))
*/
    val HASHSUM = 100000
    val tData = ctx.sparkContext.textFile(input).map {
      x => 
      val arr = x.split("\t")    
      if (arr.length != 3) {
        var els = Seq[(Int,Double)]()
        (-1, Vectors.sparse(HASHSUM, els))
      } else {
        val devid : String = arr(0)
        val title : String = arr(1)
        val sex : Int = arr(2).toString().toInt
        val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
//        val terms = HanLP.extractKeyword(title, title.length()/10).mkString(" ")
        val termarr = terms.split(" ")
        var done : Boolean = false
        var els = Seq[(Int,Double)]()
        for (i <- 0 to  termarr.length - 1) {
          val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
          if ( els.exists(x => (x._1 == tag) ) == false) {
           els = els :+ ( tag, 1D ) 
          } else 
            done = false
            for (i <- 1 to 4) {
              if (els.exists(x => (x._1 == tag + HASHSUM * i)) == false && done == false) {
                els = els :+ (tag + HASHSUM*i , 1D)
                done = true
              }
            }
          }
       (sex,Vectors.sparse(HASHSUM * 5, els))
      }
    }.filter{
      x =>
        if (x._1 == 1 || x._1 == 2) {
          true
        } else {
          false
        }
    }.randomSplit(Array(0.9,0.1))

//    val schema = StructType(Seq(StructField("sex",DataTypes.IntegerType),StructField("terms",DataTypes. )))
//    var sqlContext = new SQLContext(ctx.sparkContext)
//    var tDF =  sqlContext.createDataFrame(tData,schema).filter("sex != -1").randomSplit(Array(0.9,0.1))
    val train = tData(0)
    val test = tData(1)
    

    var trainDataRdd = train.map {
        x =>
          LabeledPoint( x._1 - 1 , x._2)
    }.cache()
    println("训练集数据量:")
    println(trainDataRdd.count())

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
    var testDataRdd = test.map{
        x =>
        LabeledPoint( x._1 -1 , x._2)
    }.cache()

    println("测试集数据量:")
    println(testDataRdd.count())


    val testpredictionAndLabel = testDataRdd.map { x =>
      (lrmodel.predict(x.features), x.label.toFloat)
    }.map {
      x=>
        var idx = (x._1 * 10).toInt
        if (x._2.toInt == 0) {
          (idx,(1,1,0))
        } else {
          (idx,(1,0,1))
        }
      }
     testpredictionAndLabel.saveAsTextFile(output)
    testpredictionAndLabel.reduceByKey {
       (x ,y) =>
         (x._1+y._1,x._2+y._2,x._3+y._3)
    }.saveAsTextFile(output2)
/*      .foreach {
        x =>
          println("idx:"+x._1 + "\tpre:"+x._2._1+"\t0:"+x._2._2+"\t1:"+ x._2._3)
      }
*/
      println("结果集数据量:" + testpredictionAndLabel.count())

//    val metrics = new BinaryClassificationMetrics(testpredictionAndLabel)
//    println("auc:"+metrics.areaUnderROC())

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("正确率：")
    println(testaccuracy)

    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}



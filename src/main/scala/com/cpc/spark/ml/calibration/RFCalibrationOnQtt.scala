package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.mllib.regression.LabeledPoint
import com.cpc.spark.common.Utils
import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}



object RFCalibrationOnQtt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.KryoSerializer.buffer.max", "2047MB")
      .appName("prepare lookalike sample".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val model = "novel-ctr-dnn-rawid-v8"
    val calimodel ="novel-ctr-dnn-rawid-v8-postcali"


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num, hour
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)

    val sample = log.rdd.map{
      r=>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid").toDouble
        val ideaid = r.getAs[Long]("ideaid").toDouble
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour").toDouble
        val features = Vectors.dense(Array(raw_ctr,adslotid,ideaid,user_req_ad_num,hour))
        LabeledPoint(label, features)
    }.toDF("label","features")

    val Array(trainingDF, testDF) = sample.randomSplit(Array(0.8, 0.2), seed = 1)

//    val sql2 = s"""
//                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num,exp_ctr
//                 | from dl_cpc.slim_union_log
//                 | where dt = '2019-05-20'
//                 | and media_appsid in ('80001098', '80001292') and isshow = 1
//                 | and ctr_model_name in ('$model','$calimodel')
//                 | and ideaid > 0 and adsrc = 1 AND userid > 0
//                 | AND (charge_type IS NULL OR charge_type = 1)
//       """.stripMargin
//    println(s"sql:\n$sql2")
//    val testsample = session.sql(sql2)
//    val test= testsample.rdd.map {
//      r =>
//        val label = r.getAs[Long]("isclick").toInt


      println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
    val nFolds: Int = 10
    val NumTrees: Int = 500
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("label_idx")
    val rf = new RandomForestClassifier()
      .setNumTrees(NumTrees)
      .setFeaturesCol("features")
      .setLabelCol("label_idx")
      .setFeatureSubsetStrategy("auto")
      .setImpurity("gini")
      .setMaxDepth(10) //2,5,7
      .setMaxBins(100)

      val predictions = lrModel.transform(testDF).select("label", "features","rawPrediction", "probability", "prediction","ideaid")
      predictions.show(5)

        //使用BinaryClassificationEvaluator来评价我们的模型
        val evaluator = new BinaryClassificationEvaluator()
        evaluator.setMetricName("areaUnderROC")
        val auc = evaluator.evaluate(predictions)
      println("model auc:%f".format(auc))
    val newprediction = lrModel.transform(test).select("label","probability","ideaid")

    //取出预测为1的probability
    val result2 = newprediction.map(line => {
      val label = line.get(line.fieldIndex("label")).toString.toInt
      val dense = line.get(line.fieldIndex("probability")).asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
      val y = dense(1).toString.toDouble * 1e6d.toInt
      val ideaid = line.get(line.fieldIndex("ideaid")).toString
      (label,y,ideaid)
    }).toDF("label","prediction","ideaid")
    //   lr calibration
    calculateAuc(result2,"lr",spark)
    //    raw data
    val modelData = testsample.selectExpr("cast(isclick as Int) label","cast(raw_ctr as Int) prediction","ideaid")
    calculateAuc(modelData,"original",spark)

//    online calibration
    val calibData = testsample.selectExpr("cast(isclick as Int) label","cast(exp_ctr as Int) prediction","ideaid")
    calculateAuc(calibData,"online",spark)

  }

 def SparseFeature(profile_num: Int)
  = udf {
    els: Seq[Row] =>
      val new_els: Seq[(Int, Double)] = els.map(x => {
        (x.getInt(0), x.getDouble(1))
      })
      Vectors.sparse(profile_num, new_els)
  }

  def calculateAuc(data:DataFrame,cate:String,spark: SparkSession): Unit ={
    val testData = data.selectExpr("cast(label as Int) label","cast(prediction as Int) score")
    val auc = CalcMetrics.getAuc(spark,testData)
    println("%s auc:%f".format(cate,auc))
    val p1= data.groupBy().agg(avg(col("label")).alias("ctr"),avg(col("prediction")).alias("ectr"))
    val ctr = p1.first().getAs[Double]("ctr")
    val ectr = p1.first().getAs[Double]("ectr")
    println("%s calibration: ctr:%f,ectr:%f,ectr/ctr:%f".format(cate, ctr, ectr/1e6d, ectr/ctr/1e6d))

    val p2 = data.groupBy("ideaid")
      .agg(avg(col("label")).alias("ctr"),avg(col("prediction")).alias("ectr"))
      .groupBy().agg(avg(col("ctr")).alias("avgctr"),avg(col("ectr")).alias("avgectr"))
    val ctr2 = p2.first().getAs[Double]("avgctr")
    val ectr2 = p2.first().getAs[Double]("avgectr")
    println("%s calibration by ideaid: avgctr:%f,avgectr:%f,avgectr/avgctr:%f".format(cate, ctr2, ectr2/1e6d, ectr2/ctr2/1e6d))
  }
}
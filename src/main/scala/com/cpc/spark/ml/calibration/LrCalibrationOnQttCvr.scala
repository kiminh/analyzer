package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{udf, _}
import com.cpc.spark.ml.calibration.LrCalibrationOnQtt.calculateAuc
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable


object LrCalibrationOnQttCvr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.KryoSerializer.buffer.max", "2047MB")
      .appName("prepare lookalike sample".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // parse and process input
    val model = "qtt-cvr-dnn-rawid-v1-180"
    val calimodel ="qtt-cvr-dnn-rawid-v1-180"

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    // get union log
    val sql = s"""
                 |select iscvr as isclick,raw_cvr as raw_ctr,exp_cvr as exp_ctr, adslotid, ideaid, user_req_ad_num,hour
                 |  from dl_cpc.qtt_cvr_calibration_sample where dt = '2019-05-20'
       """.stripMargin
    println(s"sql:\n$sql")
    val log= session.sql(sql)

    val adslotidArray = log.select("adslotid").distinct().collect()
    val ideaidArray = log.select("ideaid").distinct().collect()
    val hourArray = log.select("hour").distinct().collect()

    val adslotidID = mutable.Map[String,Int]()
    var idxTemp = 0
    val adslotid_feature = adslotidArray.map{r => adslotidID.update(r.getAs[String]("adslotid"), idxTemp); idxTemp += 1; (("adslotid"+ r.getAs[String]("adslotid")), idxTemp -1)}

    val ideaidID = mutable.Map[Long,Int]()
    var idxTemp1 = 0
    val ideaid_feature = ideaidArray.map{r => ideaidID.update(r.getAs[Long]("ideaid"), idxTemp1); idxTemp1 += 1; (( "ideaid"+ r.getAs[Long]("ideaid")), idxTemp1 -1)}

    val hourID = mutable.Map[String,Int]()
    var idxTemp2 = 0
    val hourid_feature = hourArray.map{r => hourID.update(r.getAs[String]("hour"), idxTemp2); idxTemp2 += 1; (("hour" + r.getAs[String]("hour")), idxTemp2 -1)}

    val feature_profile = adslotid_feature ++ ideaid_feature

    val adslotid_sum = adslotidID.size
    val ideaid_sum = ideaidID.size
    val hour_sum = hourID.size
    val profile_num = adslotid_sum + ideaid_sum + hour_sum +2
    val sample = log.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour")
        var els = Seq[(Int, Double)]()
        if (adslotid != null) {
          els = els :+ (adslotidID(adslotid), 1.0)
        }
        if (ideaid != null) {
          els = els :+ (ideaidID(ideaid) + adslotid_sum , 1.0)
        }
        if (hour != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hourID(hour), 1.0)
        }
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum+1 , user_req_ad_num)
        }
        (label,els,ideaid)
    }.filter(_ != null).toDF("label","els","ideaid")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"),$"ideaid")
    sample.show(5)

    val sql2 = s"""
                 |select iscvr as isclick,raw_cvr as raw_ctr,exp_cvr as exp_ctr, adslotid, ideaid, user_req_ad_num,hour
                 |  from dl_cpc.qtt_cvr_calibration_sample where dt = '2019-05-21'
       """.stripMargin
    println(s"sql:\n$sql2")
    val testsample = session.sql(sql2)
    val test= testsample.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour")
        var els = Seq[(Int, Double)]()
        if (adslotid != null) {
          if (adslotidID.contains(adslotid)){
            els = els :+ (adslotidID(adslotid), 1.0)
          }
        }
        if (ideaid != null) {
          if (ideaidID.contains(ideaid)){
            els = els :+ (ideaidID(ideaid) + adslotid_sum , 1.0)
          }
        }
        if (hour != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hourID(hour), 1.0)
        }
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum+1 , user_req_ad_num)
        }
        (label,els,ideaid)
    }.filter(_ != null).toDF("label","els","ideaid")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"),$"ideaid")
    test.show(5)

      val Array(trainingDF, testDF) = sample.randomSplit(Array(0.8, 0.2), seed = 1)
      println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
      val lrModel = new LogisticRegression().
        setLabelCol("label").
        setFeaturesCol("features").
        setMaxIter(200).
        setThreshold(0.5).
        setRegParam(0.15).
        fit(trainingDF)
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
}
package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.cpc.spark.tools.CalcMetrics
import com.cpc.spark.common.Utils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable


object LrCalibrationOnQtt {
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
    val model = "qtt-list-dnn-rawid-v4"
    val calimodel ="qtt-list-dnn-rawid-v4-postcali"


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
                 | and media_appsid in ('80000001', '80000002') and adslot_type = 1 and isshow = 1
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
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
    val hourid_feature = hourArray.map{r => hourID.update(r.getAs[String]("hour"), idxTemp1); idxTemp2 += 1; (("hour" + r.getAs[String]("hour")), idxTemp2 -1)}

    val feature_profile = adslotid_feature ++ ideaid_feature

    val adslotid_sum = adslotidID.size
    val ideaid_sum = ideaidID.size
    val profile_num = adslotid_sum + ideaid_sum + 3

    val sample = log.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour").toDouble
        var els = Seq[(Int, Double)]()
        if (adslotid != null) {
          els = els :+ (adslotidID(adslotid), 1.0)
        }
        if (ideaid != null) {
          els = els :+ (ideaidID(ideaid) + adslotid_sum , 1.0)
        }
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + 1 , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + 2 , user_req_ad_num)
        }
        if (hour != null) {
          els = els :+ (adslotid_sum + ideaid_sum + 3 , hour)
        }
        (label,els)
    }.filter(_ != null).toDF("label","els")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"))
    sample.show(5)

    val sql2 = s"""
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num,exp_ctr
                 | from dl_cpc.slim_union_log
                 | where dt = '2019-05-16' and hour ='13'
                 | and media_appsid in ('80000001', '80000002') and adslot_type = 1 and isshow = 1
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
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
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + 1 , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + 2 , user_req_ad_num)
        }
        (label,els)
    }.filter(_ != null).toDF("label","els")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"))
    test.show(5)

      val Array(trainingDF, testDF) = sample.randomSplit(Array(0.7, 0.3), seed = 1)
      println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
      val lrModel = new LogisticRegression().
        setLabelCol("label").
        setFeaturesCol("features").
        setMaxIter(200).
        setThreshold(0.5).
        setRegParam(0.15).
        fit(trainingDF)
      val predictions = lrModel.transform(testDF).select("label", "features","rawPrediction", "probability", "prediction")
      predictions.show(5)
      predictions.write.mode("overwrite").saveAsTable("test.wy00")

        //使用BinaryClassificationEvaluator来评价我们的模型
        val evaluator = new BinaryClassificationEvaluator()
        evaluator.setMetricName("areaUnderROC")
        val auc = evaluator.evaluate(predictions)
      println("model auc:%f".format(auc))
    val newprediction = lrModel.transform(test).select("label","probability")

    //取出预测为1的probability
    val result2 = newprediction.map(line => {
      val label = line.get(line.fieldIndex("label")).toString.toInt
      val dense = line.get(line.fieldIndex("probability")).asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
      val y = dense(1).toString.toDouble * 1e6d.toInt
      (label,y)
    }).toDF("label","prediction")
    //   lr calibration
    val testData = result2.selectExpr("cast(label as Int) label","cast(prediction as Int) score")
    val testauc = CalcMetrics.getAuc(spark,testData)
    println("test auc:%f".format(testauc))
      val p1= result2.groupBy().agg(avg(col("label")).alias("ctr"),avg(col("prediction")).alias("ectr"))
    val ctr = p1.first().getAs[Double]("ctr")
    val ectr = p1.first().getAs[Double]("ectr")
    println("lr calibration: ctr:%f,ectr:%f,ectr/ctr:%f".format(ctr, ectr/1e6d, ctr*1e6d/ectr))
    //    raw data
    val modelData = testsample.selectExpr("cast(isclick as Int) label","cast(raw_ctr as Int) score")
    val originalauc = CalcMetrics.getAuc(spark,modelData)
    println("original auc:%f".format(originalauc))
    val p2= modelData.groupBy().agg(avg(col("label")).alias("ctr"),avg(col("score")).alias("ectr"))
    val ctr2 = p2.first().getAs[Double]("ctr")
    val ectr2 = p2.first().getAs[Double]("ectr")
    println("original predition: ctr2:%f,ectr2:%f,ectr2/ctr2:%f".format(ctr2, ectr2/1e6d, ctr2*1e6d/ectr2))
//    online calibration
    val calibData = testsample.selectExpr("cast(isclick as Int) label","cast(exp_ctr as Int) score")
    val calibauc = CalcMetrics.getAuc(spark,modelData)
    println("original auc:%f".format(calibauc))
    val p3= modelData.groupBy().agg(avg(col("label")).alias("ctr"),avg(col("score")).alias("ectr"))
    val ctr3 = p3.first().getAs[Double]("ctr")
    val ectr3 = p3.first().getAs[Double]("ectr")
    println("online calibration predition: ctr3:%f,ectr3:%f,ectr3/ctr3:%f".format(ctr3, ectr3/1e6d, ctr3*1e6d/ectr3))

  }

 def SparseFeature(profile_num: Int)
  = udf {
    els: Seq[Row] =>
      val new_els: Seq[(Int, Double)] = els.map(x => {
        (x.getInt(0), x.getDouble(1))
      })
      Vectors.sparse(profile_num + 1, new_els)
  }
}
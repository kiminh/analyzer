package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.mllib.regression.LabeledPoint
import com.cpc.spark.common.Utils
import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable
import com.cpc.spark.ml.calibration.LrCalibrationOnQtt.calculateAuc

import scala.collection.mutable.ListBuffer

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
    val modelname = "qtt-list-dnn-rawid-v4"
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
                 | and media_appsid in ('80000001', '80000002') and isshow = 1 and adslot_type =1
                 | and ctr_model_name in ('$modelname','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)
    val adslotidArray = log.select("adslotid").distinct().collect()
    val ideaidArray = log.select("ideaid").distinct().collect()
    val hourArray = log.select("hour").distinct().collect()

    val adslotidID = mutable.Map[String,Int]()
    var idxTemp = 0
    val adslotid_feature = adslotidArray.map{r => adslotidID.update(r.getAs[String]("adslotid"), idxTemp); idxTemp += 1; (("adslotid"+ r.getAs[String]("adslotid")), idxTemp -1)}

    val ideaidID = mutable.Map[Long,Int]()
    var idxTemp1 = 0
    val ideaid_feature = ideaidArray.map{r => ideaidID.update(r.getAs[Long]("ideaid"), idxTemp1); idxTemp1 += 1; (( "ideaid"+ r.getAs[Long]("ideaid")), idxTemp1 -1)}

    val adslotid_sum = adslotidID.size
    val ideaid_sum = ideaidID.size

    var indices = List[String]("ideaidvalue","adslotidvalue","raw_ctr", "user_req_ad_num","hour")
    val data= log.rdd.map{
      r=>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour").toDouble
        val adslotidvalue = adslotidID(adslotid)
        val ideaidvalue = ideaidID(ideaid)
        (label, raw_ctr, user_req_ad_num, hour, adslotidvalue, ideaidvalue, ideaid)
    }.toDF("label","raw_ctr","user_req_ad_num","hour","adslotidvalue","ideaidvalue","ideaid")
    val sample = data.withColumn("feature",concat_ws(" ", indices.map(col): _*))
      .withColumn("label",when(col("label")===1,1).otherwise(0))
      .rdd.map{
      r=>
        val label= r.getAs[Int]("label").toDouble
        val features= r.getAs[String]("feature")
        LabeledPoint(label,Vectors.dense(features.split(' ').map(_.toDouble)))
      }
    // Split the data into training and test sets (30% held out for testing)
    val splits = sample.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    println(s"trainingDF size=${trainingData.count()},testDF size=${testData.count()}")

    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int](0 -> ideaid_sum, 1 -> adslotid_sum)
    val numTrees = 12 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = ideaid_sum + adslotid_sum + 32

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression forest model:\n" + model.toDebugString)

    val predictionDF = labelsAndPredictions.toDF("label","prediction")
      .selectExpr("cast(label as Int) label","cast(prediction*1e6d as Int) prediction")
    predictionDF.show(20)
    calculateAuc(predictionDF,"test",spark)

    val sql2 = s"""
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num,exp_ctr,hour
                 | from dl_cpc.slim_union_log
                 | where dt = '2019-05-20'
                 | and media_appsid in ('80000001', '80000002') and isshow = 1 and adslot_type =1
                 | and ctr_model_name in ('$calimodel') and hour = '22'
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
      val hour = r.getAs[String]("hour").toDouble
      var adslotidvalue = adslotid.toInt
        if(adslotidID.contains(adslotid)){
          adslotidvalue = adslotidID(adslotid)
        }
      var ideaidvalue = ideaid.toInt
      if(ideaidID.contains(ideaid)){
        ideaidvalue = ideaidID(ideaid)
      }
      (label, raw_ctr, user_req_ad_num, hour, adslotidvalue, ideaidvalue, ideaid)
    }.toDF("label","raw_ctr","user_req_ad_num","hour","adslotidvalue","ideaidvalue","ideaid")
      .withColumn("feature",concat_ws(" ", indices.map(col): _*))
      .withColumn("label",when(col("label")===1,1).otherwise(0))
      .rdd.map{
      r=>
        val label= r.getAs[Int]("label").toDouble
        val features= r.getAs[String]("feature")
        LabeledPoint(label,Vectors.dense(features.split(' ').map(_.toDouble)))
    }

    val labelsAndPredictions2 = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE2 = labelsAndPredictions2.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test2 Mean Squared Error = " + testMSE2)

    val predictionDF2 = labelsAndPredictions2.toDF("label","prediction")
      .selectExpr("cast(label as Int) label","cast(prediction*1e6d as Int) prediction")
    predictionDF2.show(20)

    calculateAuc(predictionDF2,"RF",spark)

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

//    val p2 = data.groupBy("ideaid")
//      .agg(avg(col("label")).alias("ctr"),avg(col("prediction")).alias("ectr"))
//      .groupBy().agg(avg(col("ctr")).alias("avgctr"),avg(col("ectr")).alias("avgectr"))
//    val ctr2 = p2.first().getAs[Double]("avgctr")
//    val ectr2 = p2.first().getAs[Double]("avgectr")
//    println("%s calibration by ideaid: avgctr:%f,avgectr:%f,avgectr/avgctr:%f".format(cate, ctr2, ectr2/1e6d, ectr2/ctr2/1e6d))
  }
  def nameValue(name: String) = udf((value: String) => {
    if (value == null) {
      null
    } else {
      s"$name:$value"
    }
  })

  def nameValueToIndexValue(map: mutable.LinkedHashMap[String, String]) = udf((value: String) => {
    if (value == null) {
      null
    } else {
      val array = value.split(" ")
      var list = new ListBuffer[String]
      for (a <- array) {
        val items = a.split(":")
        val feaname = items(0).trim
        if (map.contains(feaname)) {
          val newitem = map(items(0).trim) + ":" + items(1)
          list += newitem
        }
      }
      list.mkString(" ")
    }
  })
}
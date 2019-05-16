package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.cpc.spark.common.Utils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}

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
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num
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

    val adslotidID = mutable.Map[String,Int]()
    var idxTemp = 0
    val adslotid_feature = adslotidArray.map{r => adslotidID.update(r.getAs[String]("adslotid"), idxTemp); idxTemp += 1; (("adslotid" + r.getAs[Int]("adslotid")), idxTemp -1)}
    println(adslotid_feature.toString)

    val ideaidID = mutable.Map[Long,Int]()
    var idxTemp1 = 0
    val ideaid_feature = ideaidArray.map{r => ideaidID.update(r.getAs[Long]("ideaid"), idxTemp1); idxTemp1 += 1; (("ideaid" + r.getAs[Int]("ideaid")), idxTemp1 -1)}

    val feature_profile = adslotid_feature ++ideaid_feature
    val feature_table =  spark.sparkContext.parallelize(feature_profile).toDF("feature", "index")

    val adslotid_sum = adslotidID.size
    val ideaid_sum = ideaidID.size

    val sample = log.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
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
        (label,els)
    }.filter(_ != null).toDF("label", "els")
      val Array(trainingDF, testDF) = sample.randomSplit(Array(0.7, 0.3), seed = 1)
      println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
      val lrModel = new LogisticRegression().
        setLabelCol("ctr").
        setFeaturesCol("features").
        setMaxIter(10000).
        setThreshold(0.5).
        setRegParam(0.15).
        fit(trainingDF)
      val predictions = lrModel.transform(testDF).select("label", "features", "prediction")

        //使用BinaryClassificationEvaluator来评价我们的模型
        val evaluator = new BinaryClassificationEvaluator()
        evaluator.setMetricName("areaUnderROC")
        val auc = evaluator.evaluate(predictions)
      println("auc:%d".format(auc))

//    val feature = allDF.select("adslotid","ideaid")

//    val categoricalColumns = feature.columns
//    //采用Pileline方式处理机器学习流程
//    val stagesArray = new ListBuffer[PipelineStage]()
//    for (cate <- categoricalColumns) {
//      //使用StringIndexer 建立类别索引
//      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index")
//      // 使用OneHotEncoder将分类变量转换为二进制稀疏向量
//      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec")
//      stagesArray.append(indexer, encoder)
//    }
//
//    val assemblerInputs = categoricalColumns.map(_ + "classVec")
//    // 使用VectorAssembler将所有特征转换为一个向量
//    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
//
//    //使用pipeline批处理
//    val pipeline = new Pipeline()
//    pipeline.setStages(stagesArray.toArray)
//    val pipelineModel = pipeline.fit(allDF)
//    val dataset = pipelineModel.transform(allDF)
//
//    val newDF = dataset.select("click", "features", "flag")
//
//    //拆分train、test
//    val processedTrain = newDF.filter(col("flag") === 1).drop("flag")
//    val processedTest = newDF.filter(col("flag") === 2).drop("click", "flag")
//
//
//    //处理label列
//    val indexer2Click = new StringIndexer().setInputCol("click").setOutputCol("ctr")
//    val finalTrainDF = indexer2Click.fit(processedTrain).transform(processedTrain).drop("click")
//
//
//    //随机分割测试集和训练集数据
//    val Array(trainingDF, testDF) = finalTrainDF.randomSplit(Array(0.7, 0.3), seed = 1)
//    println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
//    val lrModel = new LogisticRegression().
//      setLabelCol("ctr").
//      setFeaturesCol("features").
//      setMaxIter(10000).
//      setThreshold(0.5).
//      setRegParam(0.15).
//      fit(trainingDF)
//    val predictions = lrModel.transform(testDF).select($"ctr".as("label"), "features", “rawPrediction", "probability", "prediction")
//
//      //使用BinaryClassificationEvaluator来评价我们的模型
//      val evaluator = new BinaryClassificationEvaluator()
//      evaluator.setMetricName("areaUnderROC")
//      val auc = evaluator.evaluate(predictions)
//
//
//      val newprediction = lrModel.transform(processedTest).select("probability")
//
//      //取出预测为1的probability
//      val reseult2 = newprediction.map(line => {
//      val dense = line.get(line.fieldIndex("probability")).asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
//      val y = dense(1).toString
//      (y)
//      }).toDF("pro2ture")
//
//
//
//      reseult2.repartition(1).write.text(“../firstLrResultStr")


  }
}
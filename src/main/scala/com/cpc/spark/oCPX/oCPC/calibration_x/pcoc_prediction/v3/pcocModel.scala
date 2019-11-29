package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object pcocModel {
  def main(args: Array[String]): Unit = {
    /*
    采用拟合模型进行pcoc的时序预估
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val hourDiff = args(2).toInt
    val version = args(3).toString
    val expTag = args(4).toString


    println("parameters:")
    println(s"date=$date, hour=$hour, hourDiff=$hourDiff, version=$version, expTag=$expTag")

    val data = getData(date, hour, version, expTag, spark)

    val trainingData = getTrainingData(data, spark)

    val predictData = getPredictData(date, hour, hourDiff, version, expTag, spark)

    val result = trainAndPredict(trainingData, predictData, spark)

    result
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_pred_data20191129a")

//    val resultDF = extracePredictData(result, hourDiff, spark)
//    resultDF
//      .repartition(1)
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//      .withColumn("exp_tag", lit(expTag))
////      .write.mode("overwrite").insertInto("test.ocpc_pcoc_prediction_result_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_prediction_result_hourly")

  }

  def extracePredictData(dataRaw: DataFrame, hourDiff: Int, spark: SparkSession) = {
    dataRaw.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  media,
         |  conversion_goal,
         |  conversion_from,
         |  time,
         |  $hourDiff as hour_diff,
         |  double_feature_list,
         |  string_feature_list,
         |  avg_pcoc,
         |  prediction as pred_pcoc
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getPredictData(date: String, hour: String, hourDiff: Int, version: String, expTag: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, hourDiff)
    val tmpDate1 = dateConverter.format(calendar.getTime)
    val tmpDateValue1 = tmpDate1.split(" ")
    val hour1 = tmpDateValue1(1)

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_pcoc_sample_part_hourly
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  exp_tag = '$expTag'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc6", col("double_feature_list").getItem(0))
      .withColumn("pcoc12", col("double_feature_list").getItem(1))
      .withColumn("pcoc24", col("double_feature_list").getItem(2))
      .withColumn("pcoc48", col("double_feature_list").getItem(3))
      .withColumn("pcoc72", col("double_feature_list").getItem(4))
      .withColumn("cv6", col("string_feature_list").getItem(0))
      .withColumn("cv12", col("string_feature_list").getItem(1))
      .withColumn("cv24", col("string_feature_list").getItem(2))
      .withColumn("cv48", col("string_feature_list").getItem(3))
      .withColumn("cv72", col("string_feature_list").getItem(4))
      .withColumn("hour", lit(hour1))
      .withColumn("time", concat_ws(" ", col("date"), col("hour")))
      .select("identifier", "media", "conversion_goal", "conversion_from", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72", "cv6", "cv12", "cv24", "cv48", "cv72", "hour", "time", "double_feature_list", "string_feature_list")
      .cache()

    data.show(10)

    data
  }

  def trainAndPredict(dataRaw: DataFrame, predictRawData: DataFrame, spark: SparkSession) = {
    /*
    对pre_pcoc ~ pre_cv做scaler
     */
    val data = dataRaw.cache()
    data.show(10)
    // 模型训练
    val stagesArray = new ListBuffer[PipelineStage]()

//    one-hot for hour
    val hourIndexer = new StringIndexer().setInputCol("hour").setOutputCol("hour_index")
    stagesArray.append(hourIndexer)
    val hourEncoder = new OneHotEncoder().setInputCol("hour_index").setOutputCol("hour_vec")
    stagesArray.append(hourEncoder)

//    one-hot for identifier
    val identifierIndexer = new StringIndexer().setInputCol("identifier").setOutputCol("identifier_index")
    stagesArray.append(identifierIndexer)
    val identifierEncoder = new OneHotEncoder().setInputCol("identifier_index").setOutputCol("identifier_vec")
    stagesArray.append(identifierEncoder)

//    one-hot for media
    val mediaIndexer = new StringIndexer().setInputCol("media").setOutputCol("media_index")
    stagesArray.append(mediaIndexer)
    val mediaEncoder = new OneHotEncoder().setInputCol("media_index").setOutputCol("media_vec")
    stagesArray.append(mediaEncoder)

//    one-hot for cv6
    val cv6Indexer = new StringIndexer().setInputCol("cv6").setOutputCol("cv6_index")
    stagesArray.append(cv6Indexer)
    val cv6Encoder = new OneHotEncoder().setInputCol("cv6_index").setOutputCol("cv6_vec")
    stagesArray.append(cv6Encoder)

//    one-hot for cv12
    val cv12Indexer = new StringIndexer().setInputCol("cv12").setOutputCol("cv12_index")
    stagesArray.append(cv12Indexer)
    val cv12Encoder = new OneHotEncoder().setInputCol("cv12_index").setOutputCol("cv12_vec")
    stagesArray.append(cv12Encoder)

//    one-hot for cv24
    val cv24Indexer = new StringIndexer().setInputCol("cv24").setOutputCol("cv24_index")
    stagesArray.append(cv24Indexer)
    val cv24Encoder = new OneHotEncoder().setInputCol("cv24_index").setOutputCol("cv24_vec")
    stagesArray.append(cv24Encoder)

//    one-hot for cv48
    val cv48Indexer = new StringIndexer().setInputCol("cv48").setOutputCol("cv48_index")
    stagesArray.append(cv48Indexer)
    val cv48Encoder = new OneHotEncoder().setInputCol("cv48_index").setOutputCol("cv48_vec")
    stagesArray.append(cv48Encoder)

//    one-hot for cv72
    val cv72Indexer = new StringIndexer().setInputCol("cv72").setOutputCol("cv72_index")
    stagesArray.append(cv72Indexer)
    val cv72Encoder = new OneHotEncoder().setInputCol("cv72_index").setOutputCol("cv72_vec")
    stagesArray.append(cv72Encoder)

    val featureArray = Array("identifier_vec", "media_vec", "hour_vec", "cv6_vec", "cv12_vec", "cv24_vec", "cv48_vec", "cv72_vec", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72")
    val assembler = new VectorAssembler().setInputCols(featureArray).setOutputCol("features")
    stagesArray.append(assembler)

////    standard scaler
//    val scaler = new StandardScaler()
//      .setInputCol("features")
//      .setOutputCol("scaled_features")
//      .setWithStd(true)
//      .setWithMean(false)
//    stagesArray.append(scaler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)

    val pipelineModel = pipeline.fit(data)
    val dataset = pipelineModel.transform(data)

    val dataItem = data.select("identifier", "media", "conversion_goal", "conversion_from", "hour").distinct()
    val predictFeatures = predictRawData
      .join(dataItem, Seq("identifier", "media", "conversion_goal", "conversion_from", "hour"), "inner")
    val predictData = pipelineModel.transform(predictFeatures)


    val lrModel = new LinearRegression().setFeaturesCol("features").setLabelCol("label").setRegParam(0.001).setElasticNetParam(0.1).fit(dataset)

    val predictions = lrModel
      .transform(dataset)
      .select("identifier", "media", "conversion_goal", "conversion_from", "time", "hour", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72", "cv6", "cv12", "cv24", "cv48", "cv72", "features", "double_feature_list", "string_feature_list", "prediction", "label")
//      .select("identifier", "media", "conversion_goal", "conversion_from", "time", "hour", "avg_pcoc", "diff1_pcoc", "diff2_pcoc", "recent_pcoc", "features", "double_feature_list", "string_feature_list", "prediction")
      .cache()

    predictions.show(10)
    predictions
  }

  def udfDoubleFeatures() = udf((avgPcoc: Double, diff1Pcoc: Double, diff2Pcoc: Double, recentPcoc: Double) => {
    val result = Array(avgPcoc, diff1Pcoc, diff2Pcoc, recentPcoc)
    result
  })

  def udfStringFeatures() = udf((hr: String) => {
    val result = Array(hr)
    result
  })

  def parseFeatures(rawData: DataFrame, spark: SparkSession) = {
//    udfAggregateFeature()(col("avg_pcoc"), col("diff1_pcoc"), col("diff2_pcoc"), col("recent_pcoc"))
    val dataRaw = rawData
      .withColumn("pcoc6", col("double_feature_list").getItem(0))
      .withColumn("pcoc12", col("double_feature_list").getItem(1))
      .withColumn("pcoc24", col("double_feature_list").getItem(2))
      .withColumn("pcoc48", col("double_feature_list").getItem(3))
      .withColumn("pcoc72", col("double_feature_list").getItem(4))
      .withColumn("cv6", col("string_feature_list").getItem(0))
      .withColumn("cv12", col("string_feature_list").getItem(1))
      .withColumn("cv24", col("string_feature_list").getItem(2))
      .withColumn("cv48", col("string_feature_list").getItem(3))
      .withColumn("cv72", col("string_feature_list").getItem(4))
      .withColumn("hour", col("string_feature_list").getItem(5))
      .select("identifier", "media", "conversion_goal", "conversion_from", "time", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72", "cv6", "cv12", "cv24", "cv48", "cv72", "hour", "label", "double_feature_list", "string_feature_list")

    dataRaw
  }

  def getTrainingData(rawData: DataFrame, spark: SparkSession) = {
    /*
    需要的特征包括：
    1. hour
    2. 四小时前的pcoc
    3. 四小时前的pcoc一阶差分
    4. 四小时前的pcoc二阶差分
    5. 四小时前的cv
     */
    val dataRaw = parseFeatures(rawData, spark)

    // hour特征：one-hot编码
//    val hourIndexer = new StringIndexer()
//      .setInputCol("hour")
//      .setOutputCol("hour_index")
//      .fit(dataRaw)
//    val hourIndexed = hourIndexer.transform(dataRaw)
//    val hourEncoder = new OneHotEncoder()
//      .setInputCol("hour_index")
//      .setOutputCol("hour_vec")
//    val hourFeature = hourEncoder
//      .transform(hourIndexed)
//      .select("identifier", "media", "conversion_goal", "conversion_from", "hour_vec")
//    hourFeature.show(10)

    // 数据关联
    val data = dataRaw
      .select("identifier", "media", "conversion_goal", "conversion_from", "time", "hour", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72", "cv6", "cv12", "cv24", "cv48", "cv72", "label", "double_feature_list", "string_feature_list")

    data
  }



  def getData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val selectCondition = s"`date` = '$date' and `hour` = '$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  test.ocpc_pcoc_sample_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  version = '$version'
         |AND
         |  exp_tag = '$expTag'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

}



package com.cpc.spark.oCPX.oCPC.calibration_x

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}


object OcpcPCOCpredictor{
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
    val hourInt = args(2).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt")

    val data = getTrainingData(date, hour, hourInt, spark).cache()


//    val result = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw, spark)
    data
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723b")

  }


  def getTrainingData(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    需要的特征包括：
    1. hour
    2. 四小时前的pcoc
    3. 四小时前的pcoc一阶差分
    4. 四小时前的pcoc二阶差分
    5. 四小时前的cv
     */
    val dataRaw = getRawData(date, hour, hourInt + 24, spark)

    // hour特征：one-hot编码
    val hourIndexer = new StringIndexer()
      .setInputCol("hour")
      .setOutputCol("hour_index")
      .fit(dataRaw)
    val hourIndexed = hourIndexer.transform(dataRaw)
    val hourEncoder = new OneHotEncoder()
      .setInputCol("hour_index")
      .setOutputCol("hour_vec")
    val hourFeature = hourEncoder
      .transform(hourIndexed)
      .select("identifier", "conversion_goal", "media", "time", "hour", "hour_index", "hour_vec")
    hourFeature.show(10)

    // 四小时前的pcoc及其差分计算
    val prevPcoc1 = dataRaw
      .withColumn("prev_time", udfChangeTime(4)(col("time")))
      .withColumn("origin_time", col("time"))
      .withColumn("time", col("prev_time"))
      .withColumn("pcoc1", col("pcoc"))
      .select("identifier", "conversion_goal", "media", "time", "pcoc1", "origin_time")
    prevPcoc1.show(10)

    val prevPcoc2 = dataRaw
      .withColumn("prev_time", udfChangeTime(5)(col("time")))
      .withColumn("origin_time", col("time"))
      .withColumn("time", col("prev_time"))
      .withColumn("pcoc2", col("pcoc"))
      .select("identifier", "conversion_goal", "media", "time", "pcoc2", "origin_time")
    prevPcoc2.show(10)

    val prevPcoc3 = dataRaw
      .withColumn("prev_time", udfChangeTime(6)(col("time")))
      .withColumn("origin_time", col("time"))
      .withColumn("time", col("prev_time"))
      .withColumn("pcoc3", col("pcoc"))
      .select("identifier", "conversion_goal", "media", "time", "pcoc3", "origin_time")
    prevPcoc3.show(10)

    val prevPcoc = prevPcoc1
      .join(prevPcoc2, Seq("identifier", "conversion_goal", "media", "time"), "inner")
      .join(prevPcoc3, Seq("identifier", "conversion_goal", "media", "time"), "inner")
      .select("identifier", "conversion_goal", "media", "time", "pcoc1", "pcoc2", "pcoc3")
      .withColumn("prev_pcoc", col("pcoc1"))
      .withColumn("diff1_pcoc", udfCalculateDiff1()(col("pcoc1"), col("pcoc2")))
      .withColumn("diff2_pcoc", udfCalculateDiff2()(col("pcoc1"), col("pcoc2"), col("pcoc3")))

    // 四小时的cv数
    val prevCV = dataRaw
      .withColumn("prev_time", udfChangeTime(4)(col("time")))
      .withColumn("origin_time", col("time"))
      .withColumn("time", col("prev_time"))
      .withColumn("prev_cv", col("cv"))
      .select("identifier", "conversion_goal", "media", "time", "prev_cv", "origin_time")
    prevCV.show(10)

    // 数据关联
    val data = dataRaw
        .select("identifier", "conversion_goal", "media", "time", "pcoc")
        .join(hourFeature, Seq("identifier", "conversion_goal", "media", "time"), "inner")
        .join(prevPcoc, Seq("identifier", "conversion_goal", "media", "time"), "inner")
        .join(prevCV, Seq("identifier", "conversion_goal", "media", "time"), "inner")
        .select("identifier", "conversion_goal", "media", "time", "pcoc", "hour_vec", "prev_pcoc", "diff1_pcoc", "diff2_pcoc", "prev_cv")

    dataRaw
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191107a")

    hourFeature
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191107b")

    prevPcoc
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191107c")

    prevCV
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191107d")


    data
  }

  def udfCalculateDiff2() = udf((pcoc1: Double, pcoc2: Double, pcoc3: Double) => {
    /*
    二阶差分
     */
    val result = (pcoc1 - pcoc2) - (pcoc2 - pcoc3)
    result
  })

  def udfCalculateDiff1() = udf((pcoc1: Double, pcoc2: Double) => {
    /*
    一阶差分
     */
    val result = pcoc1 - pcoc2
    result
  })

//  def OcpcCVRfactorMain(date: String, hour: String, version: String, expTag: String, dataRaw: DataFrame, spark: SparkSession) = {
//    // cvr实验配置文件
//    val data = dataRaw
//        .withColumn("media", udfMediaName()(col("media")))
//        .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
//        .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
//        .na.fill(40, Seq("min_cv"))
//        .filter(s"cv > 0")
//    data1.show(10)
//  }

  def getRawData(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result.select("identifier", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "time", "hour")


    resultDF
  }

  def udfChangeTime(hourInt: Int) = udf((time: String) => {
    val timeList = time.split(" ")
    val date = timeList(0)
    val hour = timeList(1)
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val result = date1 + " " + hour1

    result
  })

  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  cast(unitid as string) as identifier,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  cast(exp_cvr as double) as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  cast(ocpc_log_dict['cvr_factor'] as double) as cvr_factor,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }



  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .withColumn("time", concat_ws(" ", col("date"), col("hour")))
      .groupBy("identifier", "conversion_goal", "media", "time", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "time", "hour")

    data
  }



}



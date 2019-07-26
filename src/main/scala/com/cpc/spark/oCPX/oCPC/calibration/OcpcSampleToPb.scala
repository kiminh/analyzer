package com.cpc.spark.oCPX.oCPC.calibration

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools.getTimeRangeSqlDate
import com.typesafe.config.ConfigFactory
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcSampleToPb {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算

    将文件从dl_cpc.ocpc_pcoc_jfb_hourly表中抽出，存入pb文件，需要过滤条件：
    kvalue>0
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12 qtt_demo 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val fileName = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, fileName:$fileName")

    val data = getCalibrationData(date, hour, version, spark)

    val adtype15List = getAdtype15(date, hour, 48, version, spark)
    val resultDF = data
      .join(adtype15List, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
      .na.fill(1.0, Seq("ratio"))
      .withColumn("jfb_factor_old", col("jfb_factor"))
      .withColumn("jfb_factor", col("jfb_factor_old") *  col("ratio"))

    resultDF
      .select("unitid", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_param_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_pb_data_hourly")

    savePbPack(resultDF, fileName, spark)
  }

  def getCalibrationData(date: String, hour: String, version: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  is_hidden,
         |  exp_tag,
         |  cvr_factor as cali_value,
         |  jfb_factor,
         |  post_cvr,
         |  high_bid_factor,
         |  low_bid_factor,
         |  cpagiven
         |FROM
         |  dl_cpc.ocpc_pb_data_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1).cache()
    data1.show(10)

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  avg(cpa_suggest) as cpa_suggest
         |FROM
         |  dl_cpc.ocpc_history_suggest_cpa_version
         |WHERE
         |  version = 'ocpcv1'
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2).cache()
    data2.show(10)

    val data = data1
      .join(data2, Seq("unitid", "conversion_goal"), "left_outer")
      .select("unitid", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .withColumn("cali_value", udfCheckCali(0.1, 5.0)(col("cali_value")))
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor", "cpagiven"))
      .na.fill(0.0, Seq("cali_value", "jfb_factor", "post_cvr", "cpa_suggest", "smooth_factor"))

    data.show(10)

    data
  }

  def udfCheckCali(minCali: Double, maxCali: Double) = udf((caliValue: Double) => {
    var result = caliValue
    if (result < minCali) {
      result = minCali
    }
    if (result > maxCali) {
      result = maxCali
    }
    result
  })

  def savePbPack(data: DataFrame, fileName: String, spark: SparkSession): Unit = {
    /*
    oCPCQTT&unitid&isHiddenOcpc
    string   key = 1;
    int32    conversionGoal = 2;
    double   cvrCalFactor = 3;
    double   jfbFactor = 4;
    double   smoothFactor = 5;
    double   postCvr = 6;
    double   cpaGiven = 7;
    double   cpaSuggest = 8;
    double   paramT = 9;
    double   highBidFactor = 10;
    double   lowBidFactor = 11;
    int64    ocpcMincpm = 12;
    int64    ocpcMinbid = 13;
    int64    cpcbid = 14;
    int64    maxbid = 15;
     */
    var list = new ListBuffer[SingleItem]
    var cnt = 0

    for (record <- data.collect()) {
      val identifier = record.getAs[String]("identifier")
      val isHidden = record.getAs[Int]("is_hidden").toString
      val expTag = record.getAs[String]("exp_tag")
      val key = expTag + "&" + identifier + "&" + isHidden
      val conversionGoal = record.getAs[Int]("conversion_goal")
      val cvrCalFactor = record.getAs[Double]("cali_value")
      val jfbFactor = record.getAs[Double]("jfb_factor")
      val smoothFactor = record.getAs[Double]("smooth_factor")
      val postCvr = record.getAs[Double]("post_cvr")
      val cpaGiven = record.getAs[Double]("cpagiven")
      val cpaSuggest = record.getAs[Double]("cpa_suggest")
      val paramT = 2.0
      val highBidFactor = record.getAs[Double]("high_bid_factor")
      val lowBidFactor = record.getAs[Double]("low_bid_factor")
      val minCPM = 0
      val minBid = 0
      val cpcbid = 0
      val maxbid = 0

      if (cnt % 100 == 0) {
        println(s"key:$key, conversionGoal:$conversionGoal, cvrCalFactor:$cvrCalFactor, jfbFactor:$jfbFactor, smoothFactor:$smoothFactor, postCvr:$postCvr, cpaGiven:$cpaGiven, cpaSuggest:$cpaSuggest, paramT:$paramT, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, minCPM:$minCPM, minBid:$minBid, cpcbid:$cpcbid, maxbid:$maxbid")
      }
      cnt += 1

      val currentItem = SingleItem(
        key = key,
        conversionGoal = conversionGoal,
        cvrCalFactor = cvrCalFactor,
        jfbFactor = jfbFactor,
        smoothFactor = smoothFactor,
        postCvr = postCvr,
        cpaGiven = cpaGiven,
        cpaSuggest = cpaSuggest,
        paramT = paramT,
        highBidFactor = highBidFactor,
        lowBidFactor = lowBidFactor,
        ocpcMincpm = minCPM,
        ocpcMinbid = minBid,
        cpcbid = cpcbid,
        maxbid = maxbid

      )
      list += currentItem

    }
    val result = list.toArray[SingleItem]
    val adRecordList = OcpcParamsList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(fileName))

    println("complete save data into protobuffer")

  }

  def getAdtype15(date: String, hour: String, hourInt: Int, version: String, spark: SparkSession) = {
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
         |  unitid,
         |  conversion_goal,
         |  adtype
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
         |AND
         |  is_ocpc = 1
         |AND
         |  conversion_goal > 0
         |AND
         |  adtype = 15
       """.stripMargin
    println(sqlRequest)
    val data1 = spark
      .sql(sqlRequest)
      .distinct()

    val data2 = getAdtype15Factor(version, spark)

    val data = data1
      .join(data2, Seq("conversion_goal"), "inner")
      .select("unitid", "conversion_goal", "exp_tag", "adtype", "ratio")
      .cache()


    data.show(10)
    data

  }

  def getAdtype15Factor(version: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_tag.adtype15")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "ratio")
      .distinct()

    data.show(10)

    data

  }

}


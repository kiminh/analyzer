package com.cpc.spark.OcpcProtoType.model_qtt_v2

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._


object OcpcSampleToPbV2 {
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
    val media = "qtt"

//    val fileName = "ocpc_params_qtt.pb"

    val data = getCalibrationDataV2(date, hour, media, version, spark)

    data
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.ocpc_param_pb_data_hourly_v2")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_pb_data_hourly_v2")

    savePbPack(data, fileName, spark)
  }

  def getCalibrationDataV2(date: String, hour: String, media: String, version: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  is_hidden,
         |  exp_tag,
         |  1.0 / pcoc as cali_value,
         |  1.0 / jfb as jfb_factor,
         |  post_cvr,
         |  high_bid_factor,
         |  low_bid_factor,
         |  cpagiven
         |FROM
         |  dl_cpc.ocpc_param_calibration_hourly_v2
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
         |  cast(unitid as string) identifier,
         |  conversion_goal,
         |  cpa * 100.0 as cpa_suggest
         |FROM
         |  test.ocpc_qtt_light_control_v2
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2).cache()
    data2.show(10)

    val data = data1
      .join(data2, Seq("identifier", "conversion_goal"), "left_outer")
      .withColumn("smooth_factor", udfSelectSmoothFactor()(col("conversion_goal")))
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor", "cpagiven"))
      .na.fill(0.0, Seq("cali_value", "jfb_factor", "post_cvr", "cpa_suggest", "smooth_factor"))

    val result = resetSmoothFactor(data, media, spark).cache()

    result.show(10)

    result
  }

  def resetSmoothFactor(rawData: DataFrame, media: String, spark: SparkSession) = {
    // 从配置文件平滑系数
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_tag.smooth_factor")
    val rawData = spark.read.format("json").json(confPath)
    val confData = rawData
      .filter(s"media = '$media'")
      .groupBy("exp_tag", "conversion_goal")
      .agg(
        min(col("smooth_factor")).alias("conf_factor")
      )
      .selectExpr("exp_tag", "conversion_goal", "conf_factor")

    val data = rawData
      .join(confData, Seq("exp_tag", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven", "conf_factor")
      .withColumn("smooth_factor", when(col("conf_factor").isNotNull, col("conf_factor")).otherwise(col("smooth_factor")))
      .na.fill(0.0, Seq("smooth_factor"))

    data

  }

  def udfSelectSmoothFactor() = udf((conversionGoal: Int) => {
    var factor = conversionGoal match {
      case 1 => 0.2
      case 2 => 0.5
      case 3 => 0.5
      case 4 => 0.2
      case _ => 0.0
    }
    factor
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

      //      string identifier = 1;
      //      int32 conversiongoal = 2;
      //      double kvalue = 3;
      //      double cpagiven = 4;
      //      int64 cvrcnt = 5;

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

  //  def savePbPack(data: DataFrame, fileName: String, spark: SparkSession): Unit = {
  //    /*
  //    oCPCQTT&unitid&isHiddenOcpc
  //    string   key = 1;
  //    int32    conversionGoal = 2;
  //    double   cvrCalFactor = 3;
  //    double   jfbFactor = 4;
  //    double   smoothFactor = 5;
  //    double   postCvr = 6;
  //    double   cpaGiven = 7;
  //    double   cpaSuggest = 8;
  //    double   paramT = 9;
  //    double   highBidFactor = 10;
  //    double   lowBidFactor = 11;
  //    int64    ocpcMincpm = 12;
  //    int64    ocpcMinbid = 13;
  //    int64    cpcbid = 14;
  //    int64    maxbid = 15;
  //     */
  //    //    val key = "oCPCQtt&270&0"
  //    //    val conversionGoal = 3
  //    //    val cvrCalFactor = 0.5
  //    //    val jfbFactor = 1.1
  //    //    val smoothFactor = 0.5
  //    //    val postCvr = 0.032
  //    //    val cpaGiven = 1000.0
  //    //    val cpaSuggest = 1200.0
  //    //    val paramT = 2.0
  //    //    val highBidFactor = 1.1
  //    //    val lowBidFactor = 1.0
  //    //    val minCPM = 0
  //    //    val minBid = 0
  //    //    val cpcbid = 12
  //    //    val maxbid = 0
  //    var list = new ListBuffer[SingleItem]
  //    var cnt = 0
  //
  //    for (record <- data.collect()) {
  //      val identifier = record.getAs[String]("identifier")
  //      val key = "oCPCQtt&" + identifier + "&0"
  //      val conversionGoal = 3
  //      val cvrCalFactor = record.getAs[Double]("cali_value")
  //      val jfbFactor = 1.1
  //      val smoothFactor = 0.5
  //      val postCvr = record.getAs[Double]("post_cvr")
  //      val cpaGiven = 1000.0
  //      val cpaSuggest = record.getAs[Double]("cpa_suggest")
  //      val paramT = 2.0
  //      val highBidFactor = 1.1
  //      val lowBidFactor = 1.0
  //      val minCPM = 0
  //      val minBid = 0
  //      val cpcbid = 12
  //      val maxbid = 0
  //
  //      if (cnt % 100 == 0) {
  //        println(s"key:$key, conversionGoal:$conversionGoal, cvrCalFactor:$cvrCalFactor, jfbFactor:$jfbFactor, smoothFactor:$smoothFactor, postCvr:$postCvr, cpaGiven:$cpaGiven, cpaSuggest:$cpaSuggest, paramT:$paramT, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, minCPM:$minCPM, minBid:$minBid, cpcbid:$cpcbid, maxbid:$maxbid")
  //      }
  //      cnt += 1
  //
  //      //      string identifier = 1;
  //      //      int32 conversiongoal = 2;
  //      //      double kvalue = 3;
  //      //      double cpagiven = 4;
  //      //      int64 cvrcnt = 5;
  //
  //      val currentItem = SingleItem(
  //        key = key,
  //        conversionGoal = conversionGoal,
  //        cvrCalFactor = cvrCalFactor,
  //        jfbFactor = jfbFactor,
  //        smoothFactor = smoothFactor,
  //        postCvr = postCvr,
  //        cpaGiven = cpaGiven,
  //        cpaSuggest = cpaSuggest,
  //        paramT = paramT,
  //        highBidFactor = highBidFactor,
  //        lowBidFactor = lowBidFactor,
  //        ocpcMincpm = minCPM,
  //        ocpcMinbid = minBid,
  //        cpcbid = cpcbid,
  //        maxbid = maxbid
  //
  //      )
  //      list += currentItem
  //
  //    }
  //    val result = list.toArray[SingleItem]
  //    val adRecordList = OcpcParamsList(
  //      records = result
  //    )
  //
  //    println("length of the array")
  //    println(result.length)
  //    adRecordList.writeTo(new FileOutputStream(fileName))
  //
  //    println("complete save data into protobuffer")
  //
  //  }

}


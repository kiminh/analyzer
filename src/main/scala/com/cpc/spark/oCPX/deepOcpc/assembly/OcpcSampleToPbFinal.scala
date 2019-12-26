package com.cpc.spark.oCPX.deepOcpc.assembly

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbFinal {
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

    val tableName1 = "dl_cpc.ocpc_deep_param_pb_data_hourly"
    val version1 = version
    val result = getData(date, hour, tableName1, version1, spark)
    result.printSchema()

    val blackUnits = getOcpcUnits(spark)

    val resultDF = result
      .join(blackUnits, Seq("identifier", "conversion_goal", "exp_tag"), "outer")
      .withColumn("cali_value", when(col("black_flag") === 1, 0.01).otherwise(col("cali_value")))
      .withColumn("jfb_factor", when(col("black_flag") === 1, 0.01).otherwise(col("jfb_factor")))
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor", "cpagiven"))
      .na.fill(0.0, Seq("post_cvr", "cpa_suggest", "smooth_factor"))
      .na.fill(0, Seq("is_hidden"))
      .na.fill(0.01, Seq("jfb_factor", "cali_value"))
      .filter(s"is_hidden = 0")

    val finalVersion = version + "pbfile"
    resultDF
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .repartition(5)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(finalVersion))
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_deep_param_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_param_pb_data_hourly")


    savePbPack(resultDF, fileName, spark)
  }

  def getData(date: String, hour: String, tableName: String, version: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  $tableName
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("smooth_factor_old", col("smooth_factor"))
      .withColumn("smooth_factor", udfSetSmoothFactor()(col("smooth_factor")))
      .withColumn("cali_value_old", col("cali_value"))
      .withColumn("cali_value", udfCalculateCaliValue(date, hour)(col("identifier"), col("cali_value")))
      .cache()

    data
        .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191104")
    data.show(10)
    data

  }


  def udfCalculateCaliValue(date: String, hour: String) = udf((identifier: String, caliValue: Double) => {
    var result = caliValue
    val discountUnitMap = Map("2593206" ->	0.250031331, "2566057" -> 0.516086391, "2565794" -> 0.265733378, "2593089" -> 0.377641349, "2593024" -> 0.374991778)

    var tmpCali = discountUnitMap.getOrElse(identifier, 0.0)

    if (tmpCali > 0.0 && date == "2019-12-26") {
      result = tmpCali
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

    data.printSchema()

    for (record <- data.collect()) {
      val identifier = record.getAs[String]("identifier")
      val expTag = record.getAs[String]("exp_tag")
      val key = expTag + "&" + identifier
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

  def udfSetSmoothFactor() = udf((smoothFactor: Double) => {
    val result = smoothFactor match {
      case _ => 0.8
    }
    result
  })



  def getPermissionData(version: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier
         |FROM
         |  dl_cpc.ocpc_deep_white_unit_version
         |WHERE
         |  version = '$version'
         |AND
         |  flag = 1
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("flag", lit(1))
      .distinct()

    data
  }

  def getOcpcUnits(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val path = conf.getString("exp_config_v2.deep_ocpc_black_units")
    val dataRaw = spark.read.format("json").json(path)
    val data = dataRaw
      .selectExpr("identifier", "cast(conversion_goal as int) as conversion_goal", "exp_tag")
      .withColumn("black_flag", lit(1))
      .distinct()
    data.show(10)

    data
  }

}


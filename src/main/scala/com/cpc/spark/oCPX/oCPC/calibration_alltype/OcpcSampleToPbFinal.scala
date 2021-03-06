package com.cpc.spark.oCPX.oCPC.calibration_alltype

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getConversionGoalNew, getTimeRangeSqlDate, getTimeRangeSqlDay}
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

//    val tableName1 = "dl_cpc.ocpc_param_pb_data_hourly_v2"
//    val version1 = version
//    val data1 = getData(date, hour, tableName1, version1, spark)
//    data1.printSchema()

    val tableName2 = "dl_cpc.ocpc_param_pb_data_hourly"
    val version2 = version
    val data2 = getData(date, hour, tableName2, version2, spark)
    data2.printSchema()

    val tableName3 = "dl_cpc.ocpc_param_pb_data_hourly_alltype"
    val version3 = version
    val data3 = getData(date, hour, tableName3, version3, spark)
    data3.printSchema()

//    val result1 = data1
//      .selectExpr("cast(identifier as string) identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")

    val result2 = data2
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")

    val result3 = data3
      .selectExpr("cast(identifier as string) identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")




//    val result = result1.union(result2).union(result3).filter(s"is_hidden = 0")
    val result = result2.union(result3).filter(s"is_hidden = 0")
    val resultDF = setDataByConfig(result, version, date, hour, spark)

    val finalVersion = version + "pbfile"
    resultDF
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .repartition(5)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(finalVersion))
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_param_pb_data_hourly_alltype")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_pb_data_hourly_alltype")


    savePbPack(resultDF, fileName, spark)
  }

  def setDataByConfig(baseData: DataFrame, version: String, date: String, hour: String, spark: SparkSession) = {
    // smooth factor
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.unit_smooth_factor")
    println(confPath)
    val rawData = spark.read.format("json").json(confPath)
    val confData = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "identifier", "smooth_factor")
      .groupBy("exp_tag", "identifier")
      .agg(
        avg(col("smooth_factor")).alias("smooth_factor_new")
      )
      .distinct()
    confData.show(10)

    // jfb factor
    val confPath2 = conf.getString("exp_config.exptag_jfb_factor")
    val rawData2 = spark.read.format("json").json(confPath2)
    val confData2 = rawData2
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "weight")
      .groupBy("exp_tag", "conversion_goal")
      .agg(
        avg(col("weight")).alias("weight")
      )
      .select("exp_tag", "conversion_goal", "weight")

    // determine the maximum and minimum value
    val valueRange = getRangeValue(date, hour, 24, spark)


    val data = baseData
      .join(confData, Seq("exp_tag", "identifier"), "left_outer")
      .withColumn("smooth_factor_old", col("smooth_factor"))
      .withColumn("smooth_factor", when(col("smooth_factor_new").isNotNull, col("smooth_factor_new")).otherwise(col("smooth_factor")))
      .join(confData2, Seq("exp_tag", "conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("weight"))
      .withColumn("jfb_factor_old", col("jfb_factor"))
      .withColumn("jfb_factor", col("jfb_factor_old") * col("weight"))


    val result = resetCvrFactor(data, date, hour, spark)

    val resultDF = result
      .join(valueRange, Seq("identifier", "conversion_goal"), "left_outer")
      .na.fill(2.0, Seq("max_cali"))
      .na.fill(0.5, Seq("min_cali"))
      .withColumn("cali_value", udfCheckCali()(col("cali_value"), col("max_cali"), col("min_cali")))
      .cache()

    resultDF.show(10)
//    resultDF
//      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_cali_data20200120")

    resultDF
  }

  def resetCvrFactor(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val unitUserInfoRaw = getConversionGoalNew(spark)
    val unitUserInfo = unitUserInfoRaw
      .selectExpr("cast(unitid as string) as identifier", "userid")
      .withColumn("douyin_flag", lit(1))
      .filter(s"userid == 1699405")
      .distinct()
      .cache()
    unitUserInfo.show(10)

    val data = rawData
      .join(unitUserInfo, Seq("identifier"), "left_outer")
      .na.fill(0, Seq("douyin_flag"))
      .withColumn("cali_value_old", col("cali_value"))
      .withColumn("cali_ratio", udfSetCaliRatio(date, hour)(col("douyin_flag")))
      .withColumn("cali_value", col("cali_value") * col("cali_ratio"))
      .withColumn("cali_value", when(col("cali_ratio") === 0.7 && col("cali_value") > 1.0, 1.0).otherwise(col("cali_value")))

    data
  }

  def udfSetCaliRatio(date: String, hour: String) = udf((flag: Int) => {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val checkTime = dateConverter.parse("2020-01-23 06")
    val nowTime = dateConverter.parse(date + " " + hour)
    val hourDiff = (checkTime.getTime() - nowTime.getTime()) / (1000 * 60 * 60)

    val result = flag match {
      case 0 => 1.0
      case 1 => {
        if (hourDiff >= 0) {
          1.3
        } else {
          if (date == "2020-01-23") {
            0.7
          } else {
            1.0
          }
        }
      }
      case _ => 1.0
    }
    result
  })

//  def udfCheckCvrFactorDiscount(date: String) = udf((identifier: String) => {
//    val idList = identifier.split("&")
//    val unitId = idList(0).toInt
//    val discountUnitMap = Map(2706111 -> 0.766429284, 2706174 -> 0.884534881, 2706209 -> 0.682934968, 2706385 -> 0.897888865, 2706469 -> 0.975722865, 2706500 -> 0.566258024, 2706543 -> 0.877753195, 2706566 -> 0.861109459, 2706645 -> 0.677304366)
//
//    var result = discountUnitMap.getOrElse(unitId, 1.0)
//    if (date != "2020-01-20") {
//      result = 1.0
//    }
//    result
//  })

  def getRangeValue(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val tableName = "dl_cpc.ocpc_base_unionlog"

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
         |  conversion_goal
         |FROM
         |  $tableName
         |WHERE
         |  $selectCondition
         |AND
         |  is_ocpc = 1
         |AND
         |  site_type = 1
         |GROUP BY unitid, conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .selectExpr("cast(unitid as string) identifier", "conversion_goal")
      .withColumn("max_cali", lit(2.0))
      .withColumn("min_cali", lit(0.5))

    data
  }

  def udfCheckCali() = udf((caliValue: Double, maxValue: Double, minValue: Double) => {
    var result = caliValue
    if (result < minValue) {
      result = minValue
    }
    if (result > maxValue) {
      result = maxValue
    }
    result
  })

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
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data

  }

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


}


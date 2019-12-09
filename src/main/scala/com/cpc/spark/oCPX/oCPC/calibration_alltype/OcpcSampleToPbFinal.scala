package com.cpc.spark.oCPX.oCPC.calibration_alltype

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, getTimeRangeSqlDay}
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
      .withColumn("cali_value_before_change", col("cali_value")) // todo: 手动调整校准系数
      .withColumn("cali_value", udfCheckCaliMaxById(date, hour)(col("identifier"), col("exp_tag"), col("cali_value")))
      .withColumn("jfb_factor_old", col("jfb_factor"))
      .withColumn("jfb_factor", col("jfb_factor_old") * col("weight"))
      .join(valueRange, Seq("identifier", "conversion_goal"), "left_outer")
      .na.fill(2.0, Seq("max_cali"))
      .na.fill(0.5, Seq("min_cali"))
      .withColumn("cali_value_old", col("cali_value"))
      .withColumn("cali_value", udfCheckCali()(col("cali_value"), col("max_cali"), col("min_cali")))
      .cache()

    data.show(10)
//    data
//      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_smooth_data20190828")

    data
  }

  def udfCheckCaliMaxById(date: String, hour: String) = udf((identifier: String, expTag: String, caliValue: Double) => {
    val idList = identifier.split("&")
    val unitid = idList(0).toInt
    val hourInt = hour.toInt

    var result = caliValue
    if (unitid == 2283585 && expTag == "adtype15MiDu" && date == "2019-12-05" && hourInt < 13) {
      result = math.min(caliValue, 1.0)
    }
    result
  })

  def udfCheckCvrFactorDiscount(date: String) = udf((identifier: String) => {
    val idList = identifier.split("&")
    val unitId = idList(0).toInt
    val discountUnitMap = Map(2493065 ->	0.304738457, 2488541 ->	0.384347388, 2486507 ->	0.42602729, 2487900 ->	0.447122503, 2484414 ->	0.447202319, 2450185 ->	0.47290526, 2493128 ->	0.517055558, 2489590 ->	0.549835833, 2401313 ->	0.573311281, 2488469 ->	0.579391844, 2453436 ->	0.602415542, 2442775 ->	0.602686077, 2456177 ->	0.608504897, 2338669 ->	0.621816766, 2294346 ->	0.622079719, 2487891 ->	0.626703325, 2489583 ->	0.630905049, 2496421 ->	0.661265109, 2489917 ->	0.668466259, 2438511 ->	0.673274774, 2275227 ->	0.673672127, 2489914 ->	0.677809855, 2414304 ->	0.695988806, 2493106 ->	0.696439203, 2494781 ->	0.699595541, 2476841 ->	0.711377215, 2388977 ->	0.721823988, 2473035 ->	0.727914746, 2457167 ->	0.733262482, 2434622 ->	0.737438087, 2481026 ->	0.744963753, 2489921 ->	0.746047398, 2476971 ->	0.748907116, 2393799 ->	0.760969542, 2431615 ->	0.771227777, 2492944 ->	0.773539966, 2472825 ->	0.782309092, 2466742 ->	0.797097353)

    var result = discountUnitMap.getOrElse(unitId, 1.0)
    if (date != "2019-11-06") {
      result = 1.0
    }
    result
  })

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


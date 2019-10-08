package com.cpc.spark.oCPX.deepOcpc.assembly

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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
    val hourInt = args(3).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, hourInt:$hourInt")

    val resultDF = getCalibrationData(date, hour, version, hourInt, spark)
//
//    val resultDF = data
//      .join(adtype15List, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
//      .na.fill(1.0, Seq("ratio"))
//      .withColumn("jfb_factor_old", col("jfb_factor"))
//      .withColumn("jfb_factor", col("jfb_factor_old") *  col("ratio"))

    resultDF
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_deep_param_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_param_pb_data_hourly")

  }

  def getCalibrationData(date: String, hour: String, version: String, hourInt: Int, spark: SparkSession) = {
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

    val sqlRequest1 =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  is_hidden,
         |  exp_tag,
         |  cvr_factor as cali_value,
         |  jfb_factor,
         |  post_cvr,
         |  smooth_factor,
         |  high_bid_factor,
         |  low_bid_factor,
         |  cpagiven,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_deep_pb_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .withColumn("create_time", concat_ws(" ", col("date"), col("hour")))
      .withColumn("time_stamp", unix_timestamp(col("create_time"), "yyyy-MM-dd HH"))
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  *,
         |  row_number() over(partition by identifier, conversion_goal, is_hidden, exp_tag order by time_stamp desc) as seq
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val dataRaw1 = spark.sql(sqlRequest2)

    val data1 = dataRaw1
      .filter(s"seq = 1")
      .cache()
    data1.show(10)

//    val sqlRequest3 =
//      s"""
//         |SELECT
//         |  unitid,
//         |  conversion_goal,
//         |  avg(cpa_suggest) as cpa_suggest
//         |FROM
//         |  dl_cpc.ocpc_history_suggest_cpa_version
//         |WHERE
//         |  version = 'ocpcv1'
//         |GROUP BY unitid, conversion_goal
//       """.stripMargin
//    println(sqlRequest3)
//    val data2 = spark.sql(sqlRequest3).cache()
//    data2.show(10)

    val data = data1
      .withColumn("cpa_suggest", lit(0.0))
      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven", "unitid")
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

}


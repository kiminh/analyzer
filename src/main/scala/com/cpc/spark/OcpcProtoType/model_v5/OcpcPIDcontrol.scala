package com.cpc.spark.OcpcProtoType.model_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcPIDcontrol {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val conversionGoal = args(4).toInt
    val sampleHour = args(5).toInt
    val minCV = args(6).toInt
    val kp = args(7).toDouble
    val ki = args(8).toDouble
    val kd = args(9).toDouble

    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, version=$version, conversionGoal=$conversionGoal, sampleHour=$sampleHour, minCV=$minCV, kp=$kp, ki=$ki, kd=$kd")

    val baseData = getBaseData(media, sampleHour, conversionGoal, date, hour, spark)
//    val errorData = calculateError(baseData, minCV, date, hour, spark)
//    val prevError = getPrevData(sampleHour, conversionGoal, version, date, hour, spark)
//
//    val data = errorData
//      .join(prevError, Seq("unitid"), "left_outer")
//      .select("unitid", "current_error", "prev_error", "last_error")
//
//    val result = calculatePID(data, kp, ki, kd, date, hour, spark)
//
//    val resultDF = result
//      .select("unitid", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment")
//      .withColumn("conversion_goal", lit(conversionGoal))
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//
//    resultDF
////      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_pid_error_data_hourly")
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pid_error_data_hourly")
//  }
//
//  def calculatePID(baseData: DataFrame, kp: Double, ki: Double, kd: Double, date: String, hour: String, spark: SparkSession) = {
//    val result = baseData
//      .na.fill(0, Seq("current_error", "prev_error", "last_error"))
//      .withColumn("increment", udfCalculatePID(kp, ki, kd)(col("current_error"), col("prev_error"), col("last_error")))
//      .withColumn("kp", lit(kp))
//      .withColumn("ki", lit(ki))
//      .withColumn("kd", lit(kd))
//      .select("unitid", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment")
//
//    result
  }

  def udfCalculatePID(kp: Double, ki: Double, kd: Double) = udf((currentError: Double, prevError: Double, lastError: Double) => {
    var result = 0.0
    if (currentError == 0 || prevError == 0 || lastError == 0) {
      result = 0.0
    } else {
      result = kp * currentError - ki * prevError + kd * lastError
    }
    result
  })

  def getPrevData(hourInt: Int, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
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

    // 抽取数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  current_error as error1,
         |  prev_error as error2
         |FROM
         |  dl_cpc.ocpc_pid_error_data_hourly
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '$hour1'
         |AND
         |  version = '$version'
         |AND
         |  conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest1)
    val data = spark
        .sql(sqlRequest1)
        .withColumn("prev_error", col("error1"))
        .withColumn("last_error", col("error2"))

    data
  }

  def calculateError(baseData: DataFrame, minCV: Int, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpareal,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    val data = spark.sql(sqlRequest1)

    val currentError = data
      .filter(s"cv >= $minCV")
      .select("unitid", "cpareal", "cpagiven", "cv")
      .withColumn("current_error", udfCalculateError()(col("cpagiven"), col("cpareal")))

    currentError
  }

  def udfCalculateError() = udf((cpagiven: Double, cpareal: Double) => {
    val result = 1.0 - cpareal / cpagiven
    result
  })


  def getBaseData(media: String, sampleHour: Int, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -sampleHour)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 点击数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  exp_cvr
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  conversion_goal = $conversionGoal
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    // 转化数据
    val cvrType = "cvr" + conversionGoal.toString
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)

    // 数据关联
    val data = clickData
      .join(cvData, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    data
  }
}


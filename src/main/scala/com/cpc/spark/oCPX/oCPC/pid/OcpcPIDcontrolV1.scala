package com.cpc.spark.oCPX.oCPC.pid

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcPIDcontrolV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val hourInt = args(3).toInt
    val minCV = args(4).toInt
    val kp = args(5).toDouble
    val ki = args(6).toDouble
    val kd = args(7).toDouble
    val expTag = args(8).toString


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, hourInt=$hourInt, minCV=$minCV, kp=$kp, ki=$ki, kd=$kd, exptag=$expTag")

    val baseData = getBaseData(hourInt, date, hour, spark)
    val errorData = calculateError(baseData, date, hour, spark)
    val prevError = getPrevData(hourInt, expTag, version, date, hour, spark)
    val prevCali = getPrevCali(baseData, date, hour, spark)

    val data = errorData
      .join(prevError, Seq("identifier", "conversion_goal", "media"), "left_outer")
      .select("identifier", "conversion_goal", "media", "current_error", "prev_error", "last_error", "cv")
      .join(prevCali, Seq("identifier", "conversion_goal", "media"), "left_outer")
      .select("identifier", "conversion_goal", "media", "current_error", "prev_error", "last_error", "cv", "prev_cali")
      .na.fill(1.0, Seq("prev_cali"))
      .na.fill(0.0, Seq("prev_error", "last_error"))

    val pidData = calculatePID(data, kp, ki, kd, minCV, date, hour, spark)

    val pidResult = pidData
      .select("identifier", "conversion_goal", "media", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment_value", "prev_cali", "current_cali")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("version", lit(version))
      .cache()
//
    pidResult
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_pid_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pid_data_hourly")

    println("successfully save data into hive")
  }

  def calculatePID(baseData: DataFrame, kp: Double, ki: Double, kd: Double, minCV: Int, date: String, hour: String, spark: SparkSession) = {
    val result = baseData
      .na.fill(0, Seq("current_error", "prev_error", "last_error"))
      .withColumn("increment_value", udfCalculatePID(kp, ki, kd)(col("current_error"), col("prev_error"), col("last_error")))
      .withColumn("kp", lit(kp))
      .withColumn("ki", lit(ki))
      .withColumn("kd", lit(kd))
      .withColumn("current_calivalue", udfUpdateCali(minCV)(col("increment_value"), col("prev_cali"), col("cv")))
      .select("identifier", "conversion_goal", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment_value", "current_calivalue", "cv", "online_cali")

    result
  }

  def udfUpdateCali(minCV: Int) = udf((increment: Double, prevCali: Double, cv: Int) => {
    var currentCali = prevCali + increment
    if (currentCali < 0.2) {
      currentCali = 0.2
    }
    if (currentCali > 2.0) {
      currentCali = 2.0
    }
    if (cv < minCV) {
      currentCali = prevCali
    }
    currentCali
  })

  def udfCalculatePID(kp: Double, ki: Double, kd: Double) = udf((currentError: Double, prevError: Double, lastError: Double) => {
    var result = kp * (currentError - prevError) + ki * currentError + kd * (currentError - 2.0 * prevError + lastError)
    result
  })

  def getPrevCali(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  media,
         |  sum(case when isclick=1 then bid * 1.0 / (cpagiven * exp_cvr) else 0 end) * 1.0 / sum(isclick) as prev_cali
         |FROM
         |  base_data
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |GROUP BY identifier, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getPrevData(hourInt: Int, expTag: String, version: String, date: String, hour: String, spark: SparkSession) = {
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
         |  identifier,
         |  conversion_goal,
         |  media,
         |  current_error as error1,
         |  prev_error as error2
         |FROM
         |  test.ocpc_pid_data_hourly
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '$hour1'
         |AND
         |  version = '$version'
         |AND
         |  exp_tag = '$expTag'
       """.stripMargin
    println(sqlRequest1)
    val data = spark
        .sql(sqlRequest1)
        .withColumn("prev_error", col("error1"))
        .withColumn("last_error", col("error2"))

    data
  }

  def calculateError(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  media,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpareal,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY identifier, conversion_goal, media
       """.stripMargin
    val data = spark.sql(sqlRequest1)

    val currentError = data
      .select("identifier", "conversion_goal", "media", "cpareal", "cpagiven", "cv")
      .withColumn("current_error", udfCalculateError()(col("cpagiven"), col("cpareal")))

    currentError
  }

  def udfCalculateError() = udf((cpagiven: Double, cpareal: Double) => {
    val result = 1.0 - cpareal / cpagiven
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
         |  cast(unitid as string) identifier,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr,
         |  media_appsid,
         |  conversion_goal,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
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
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }
}


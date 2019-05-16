package com.cpc.spark.OcpcProtoType.model_v3.pid

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcPIDmerge {
  def main(args: Array[String]): Unit = {
    /*
    采用pid方式进行校准
    1. 抽取前72小时分单元、转化目标的有消费记录
    2. 抽取当前小时的增量k值
    3. 抽取上一次的生效k值，并更新k值
    4. 对于上一次不存在的值，以1为初始值进行更新
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString
    val sampleHour = args(4).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, version=$version, sampleHour=$sampleHour")

    val ocpcRecord = getOcpcRecord(media, date, hour, spark)
    val incrementK = getIncrementK(version, date, hour, spark)
    val prevK = getPrevK(version, sampleHour, date, hour, spark)

    val data = updateK(prevK, incrementK, ocpcRecord, date, hour, spark)

    data
//      .repartition(5).write.mode("overwrite").saveAsTable("test.check_data_20190514")
      .repartition(5).write.mode("overwrite").saveAsTable("test.check_data_20190514")

    val result = data
      .select("unitid", "conversion_goal", "kvalue")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    result
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pid_k_data_hourly")
  }

  def updateK(prevK: DataFrame, incrementK: DataFrame, ocpcRecord: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    更新k值:
    1. 如果pid的更新值为空，则increment_k=0，继续更新
    2. 如果上一次的k值不存在，则prev_k = 1，继续更新
    3. 更新规则: update_k = prev_k + increment_k
     */
    val data = ocpcRecord
      .join(prevK, Seq("unitid", "conversion_goal"), "left_outer")
      .join(incrementK, Seq("unitid", "conversion_goal"), "left_outer")
      .select("unitid", "conversion_goal", "prev_k", "increment_k")
      .na.fill(1, Seq("prev_k"))
      .na.fill(0, Seq("increment_k"))
      .withColumn("kvalue", udfCalculateKvalue(0.2, 5.0)(col("prev_k"), col("increment_k")))

    data
  }

  def udfCalculateKvalue(lowerBound: Double, upperBound: Double) = udf((prevK: Double, incrementK: Double) => {
    val result = prevK + incrementK
    var updateK = 0.0
    if (result < lowerBound) {
      updateK = lowerBound
    } else if (result > upperBound) {
      updateK = upperBound
    } else {
      updateK = result
    }
    updateK
  })

  def getPrevK(version: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  kvalue as prev_k
         |FROM
         |  dl_cpc.ocpc_pid_k_data_hourly
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '$hour1'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getIncrementK(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  increment as increment_k
         |FROM
         |  dl_cpc.ocpc_pid_error_data_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getOcpcRecord(media: String, date: String, hour: String, spark: SparkSession) = {
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
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 点击数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  conversion_goal,
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
    val data = spark
      .sql(sqlRequest)
      .filter("is_hidden = 0")
      .select("unitid", "conversion_goal")
      .distinct()

    data
  }
}

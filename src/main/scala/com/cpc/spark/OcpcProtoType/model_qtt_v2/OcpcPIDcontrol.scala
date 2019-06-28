package com.cpc.spark.OcpcProtoType.model_qtt_v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.model_v5.OcpcSmoothFactor.OcpcSmoothFactorMain
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v5.OcpcPIDcontrol._

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
    val expTag = args(10).toString
    val hourInt = args(11).toInt
    val isHidden = 0


    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, version=$version, conversionGoal=$conversionGoal, sampleHour=$sampleHour, minCV=$minCV, kp=$kp, ki=$ki, kd=$kd, exptag=$expTag")

    val baseData = getBaseData(media, sampleHour, conversionGoal, date, hour, spark)
    val errorData = calculateError(baseData, date, hour, spark)
    val prevError = getPrevData(sampleHour, conversionGoal, expTag, isHidden, version, date, hour, spark)

    val data = errorData
      .join(prevError, Seq("unitid"), "left_outer")
      .select("unitid", "current_error", "prev_error", "last_error", "online_cali", "prev_cali", "cv")
      .na.fill(1.0, Seq("prev_cali"))
      .na.fill(0.0, Seq("prev_error", "last_error"))

    val pidData = calculatePID(data, kp, ki, kd, minCV, date, hour, spark)

    val pidResult = pidData
      .select("unitid", "current_error", "prev_error", "last_error", "kp", "ki", "kd", "increment_value", "current_calivalue")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("version", lit(version))
      .cache()

    pidResult
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_pid_cali_data_hourly")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pid_cali_data_hourly")

    val cvrType = "cvr" + conversionGoal.toString
    val cvrData = OcpcSmoothFactorMain(date, hour, version, media, hourInt, cvrType, spark)
    val otherData = cvrData
      .select("identifier", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .selectExpr("cast(identifier as int) unitid", "jfb", "post_cvr")
      .cache()
    otherData.show(10)

    val result = otherData
      .join(pidResult, Seq("unitid"), "left_outer")
      .selectExpr("cast(unitid as string) identifier", "jfb", "post_cvr", "current_calivalue")
      .na.fill(1.0, Seq("current_calivalue"))
      .withColumn("pcoc", udfGetCountDown()(col("current_calivalue")))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .withColumn("cpagiven", lit(1.0))
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven")
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
//      .write.mode("overwrite").saveAsTable("test.ocpc_param_calibration_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly_v2")



    println("successfully save data into hive")


  }
}


package com.cpc.spark.OcpcProtoType.model_v5

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.model_v5.OcpcSmoothFactor.OcpcSmoothFactorMain
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbPID {
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

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val highBidFactor = args(4).toDouble
    val lowBidFactor = args(5).toDouble
    val hourInt = args(6).toInt
    val conversionGoal = args(7).toInt
    val minCV = args(8).toInt
    val expTag = args(9).toString
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(10).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(11).toInt
    // 兜底校准时长
    val hourInt3 = args(12).toInt
    //    val fileName = "ocpc_params_qtt.pb"

//    val data1 = OcpcSmoothFactorMain(date, hour, version, media, hourInt1, cvrType, spark).cache()
//
////    val data = getCalibrationData(date, hour, version, spark)
////
////    data
////      .select("identifier", "conversion_goal", "is_hidden", "exp_tag", "cali_value", "jfb_factor", "post_cvr", "high_bid_factor", "low_bid_factor", "cpa_suggest", "smooth_factor", "cpagiven")
////      .withColumn("date", lit(date))
////      .withColumn("hour", lit(hour))
////      .withColumn("version", lit(version))
////      .repartition(5)
////      //        .write.mode("overwrite").saveAsTable("test.ocpc_param_pb_data_hourly_v2")
////      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_pb_data_hourly_v2")
  }

}


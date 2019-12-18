package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object assessModel {
  def main(args: Array[String]): Unit = {
    /*
    采用拟合模型进行pcoc的时序预估
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(3).toString
    val expTag = args(4).toString


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag=$expTag")

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |    a.unitid,
         |    a.userid,
         |    a.media,
         |    a.conversion_goal,
         |    a.date,
         |    a.hour,
         |    a.pre_cvr,
         |    a.raw_cvr,
         |    a.cali_precvr,
         |    a.post_cvr,
         |    a.cv,
         |    a.pre_cvr * 1.0 / a.post_cvr as pcoc,
         |    b.avg_pcoc,
         |    b.recent_pcoc,
         |    b.pred_pcoc
         |FROM
         |    (SELECT
         |        unitid,
         |        userid,
         |        media,
         |        conversion_goal,
         |        date,
         |        hour,
         |        sum(total_precvr) * 1.0 / sum(click) as pre_cvr,
         |        sum(total_rawcvr) * 1.0 / sum(click) as raw_cvr,
         |        sum(total_calipcvr) * 1.0 / sum(click) as cali_precvr,
         |        sum(cv) * 1.0 / sum(click) as post_cvr,
         |        sum(cv) as cv
         |    FROM
         |        dl_cpc.ocpc_report_base_hourly
         |    WHERE
         |        $selectCondition
         |    GROUP BY unitid, userid, media, conversion_goal, date, hour) as a
         |INNER JOIN
         |    (SELECT
         |        cast(identifier as int) as unitid,
         |        media,
         |        conversion_goal,
         |        split(time, ' ')[0] as date,
         |        split(time, ' ')[1] as hour,
         |        avg_pcoc,
         |        double_feature_list[3] as recent_pcoc,
         |        pred_pcoc
         |    FROM dl_cpc.ocpc_pcoc_prediction_result_hourly
         |    where date >= '$date1'
         |    and version = '$version'
         |    and exp_tag = '$expTag') as b
         |ON
         |    a.unitid = b.unitid
         |AND
         |    a.media = b.media
         |AND
         |    a.conversion_goal = b.conversion_goal
         |AND
         |    a.date = b.date
         |AND
         |    a.hour = b.hour
         |""".stripMargin
    println(sqlRequest)
    val dataRaw = spark
      .sql(sqlRequest)
      .withColumn("avg_diff", abs(col("pcoc") - col("avg_pcoc")))
      .withColumn("recent_diff", abs(col("pcoc") - col("recent_pcoc")))
      .withColumn("pred_diff", abs(col("pcoc") - col("pred_pcoc")))

    val data = dataRaw
      .groupBy("media", "conversion_goal")
      .agg(
        avg(col("avg_diff")).alias("avg_mae"),
        avg("recent_diff").alias("recent_mae"),
        avg("pred_diff").alias("pred_mae")
      )
      .select("media", "conversion_goal", "avg_mae", "recent_mae", "pred_mae")

    data
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_pcoc_pred_result_method20191124")

  }



}



package com.cpc.spark.oCPX.deepOcpc.report

import com.cpc.spark.oCPX.deepOcpc.DeepOcpcTools._
import com.cpc.spark.ocpcV3.utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")

    val result1 = OcpcCalculateAUCmain(date, hour, hourInt, spark)

    result1
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_deep_ocpc_auc20191028")

  }

  def OcpcCalculateAUCmain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getDeepDataDelay(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateAUC(baseData, spark)

    val resultDF = result
      .select("identifier", "auc", "date")


    resultDF
  }

  def calculateAUC(data: DataFrame, spark: SparkSession) = {
    import spark.implicits._

    val newData = data
      .withColumn("key", concat_ws("-", col("identifier"), col("date")))
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .selectExpr("key", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "key")
    val resultRDD = result.rdd.map(row => {
      val id = row.getAs[String]("name")
      val identifierList = id.trim.split("-")
      val identifier = identifierList(0)
      val date = identifierList(1)
      val auc = row.getAs[Double]("auc")
      (identifier, auc, date)
    })
    val resultDF = resultRDD.toDF("identifier", "auc", "date")
    resultDF
  }

}
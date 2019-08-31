package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.ocpcV3.utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcClickAucReport {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    println("parameters:")
    println(s"date=$date, hour=$hour")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyAucReport: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getData(date, hour, spark)

    // 详情表数据

    val data = calculateAUCbyHour(rawData, date, hour, spark)
    data.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190820")


  }

  def getData(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date` = '$date' and `hour` <= '$hour'"

    // ctrData
    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    isclick,
         |    isshow,
         |    exp_ctr,
         |    date,
         |    hour
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |and is_ocpc=1
         |and media_appsid in ('80000001', '80000002')
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
       """.stripMargin
    println(sqlRequest)
    val ctrData = spark
      .sql(sqlRequest)


    ctrData


  }

  def calculateAUCbyHour(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("unitid", "hour").distinct()
    import spark.implicits._

    val newData = data
      .withColumn("identifier", concat_ws("-", col("unitid"), col("hour")))
      .withColumn("score", col("exp_ctr") * 1000000)
      .withColumn("label", col("isclick"))
      .selectExpr("identifier", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "identifier")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val identifierList = identifier.trim.split("-")
      val unitid = identifierList(0).toInt
      val hour = identifierList(1).toString
      val auc = row.getAs[Double]("auc")
      (unitid, hour, auc)
    })
    val resultDF = resultRDD.toDF("unitid", "hour", "auc")
    resultDF
  }

}
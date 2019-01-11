package com.cpc.spark.ocpcV3.ocpcNovel.report

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.cpc.spark.ocpcV3.utils

import scala.collection.mutable

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"ocpcNovel.report.OcpcCalculateAUC: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(date, hour, spark)

    // 详情表数据
    val unitData1 = calculateByUnitid(rawData, date, hour, spark)
    val unitData2 = calculateAUCbyUserid(rawData, date, hour, spark)
    val unitData = unitData1
      .join(unitData2, Seq("userid", "conversion_goal"), "left_outer")
      .select("ideaid", "userid", "conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")

    unitData.write.mode("overwrite").saveAsTable("test.ocpc_novel_detail_report_hourly20190111")

    // 汇总表数据
    val conversionData1 = calculateByConversionGoal(rawData, date, hour, spark)
    val conversionData2 = calculateAUCbyConversionGoal(rawData, date, hour, spark)
    val conversionData = conversionData1
      .join(conversionData2, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")

    conversionData.write.mode("overwrite").saveAsTable("test.ocpc_novel_summary_report_hourly20190111")


  }

  def getOcpcLog(date: String, hour: String, spark: SparkSession) = {
    val selectCondition1 = s"`date` = '$date' and `hour` <= '$hour'"
    val selectCondition2 = s"`date` = '$date' and `hour` <= '$hour'"

    // ctrData
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isclick,
         |  isshow,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['dynamicbid'] as double) as bid,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  price
         |FROM
         |  dl_cpc.ocpcv3_unionlog_label_hourly
         |WHERE
         |  $selectCondition1
       """.stripMargin
    println(sqlRequest)
    val ctrData = spark.sql(sqlRequest)

    // cvr1Data
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  label2 as iscvr1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  $selectCondition2
         |AND
         |  label2=1
         |AND
         |  label_type!=12
       """.stripMargin
    println(sqlRequest1)
    val cvr1Data = spark.sql(sqlRequest1).distinct()

    // cvr2Data
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val cvr2Data = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr1", "iscvr2")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(col("iscvr2")))
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr")
      .na.fill(0, Seq("iscvr"))

    resultDF


  }

  def calculateByUnitid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    //    预测cvr
    //    acb
    //    auc
    //    q_factor
    //    suggest_cpa
    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 100.0 / sum(isclick) as pre_cvr,
         |  sum(iscvr) * 100.0 / sum(isclick) as post_cvr,
         |  0 as q_factor,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpareal,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def calculateByConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    //    预测cvr
    //    acb
    //    auc
    //    q_factor(目前是0）
    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  conversion_goal,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 100.0 / sum(isclick) as pre_cvr,
         |  sum(iscvr) * 100.0 / sum(isclick) as post_cvr,
         |  0 as q_factor,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpareal,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb
         |FROM
         |  base_data
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def calculateAUCbyUserid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val newData = data
      .withColumn("identifier", concat_ws("-", col("userid"), col("conversion_goal")))
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .selectExpr("identifier", "cast(score as int) score", "label")
    val result = utils.getGauc(spark, newData, "identifier")
    result.show(10)
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val identifierList = identifier.trim.split("-")
      val userid = identifierList(0).toInt
      val conversionGoal = identifierList(1).toInt
      val auc = row.getAs[Double]("auc")
      (userid, conversionGoal, auc)
    })
    val resultDF = resultRDD.toDF("userid", "conversion_goal", "auc")
    println("calcualte by userid:")
    resultDF.show(10)
    resultDF

  }

  def calculateAUCbyConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val newData = data
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .select("conversion_goal", "cast(score as int) score", "label")
    val result = utils.getGauc(spark, newData, "conversion_goal")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name").toInt
      val auc = row.getAs[Double]("auc")
      (identifier, auc)
    })
    val resultDF = resultRDD.toDF("conversion_goal", "auc")
    println("calcualte by conversion_goal:")
    resultDF.show(10)
    resultDF

  }

}

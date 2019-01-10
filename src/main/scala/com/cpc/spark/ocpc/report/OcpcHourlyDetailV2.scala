package com.cpc.spark.ocpc.report

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object OcpcHourlyDetailV2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyDetailV2: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(date, hour, spark)

    // 详情表数据
    val ideaidData1 = calculateByIdeaid(rawData, date, hour, spark)
    val ideaidData2 = calculateAUCbyIdeaid(rawData, date, hour, spark)
    val ideaidData = ideaidData1
      .join(ideaidData2, Seq("ideaid", "userid", "conversion_goal"), "left_outer")
      .select("ideaid", "userid", "conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")

    // 汇总表数据
    val conversionData1 = calculateByConversionGoal(rawData, date, hour, spark)
    val conversionData2 = calculateAUCbyConversionGoal(rawData, date, hour, spark)
    val conversionData = conversionData1
      .join(conversionData2, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")

    // 存储数据
    ideaidData.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190109")
    conversionData.write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly20190109")


  }

  def getOcpcLog(date: String, hour: String, spark: SparkSession) = {
    val selectCondition1 = s"`dt` = '$date' and `hour` <= '$hour'"
    val selectCondition2 = s"`date` = '$date' and `hour` <= '$hour'"

    // ctrData
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  userid,
         |  isclick,
         |  isshow,
         |  exp_cvr * 1.0 / 1000000 as exp_cvr,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['dynamicbid'] as double) as bid,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  price
         |FROM
         |  dl_cpc.ocpc_unionlog
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

    // cvr3Data
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr3
         |FROM
         |  dl_cpc.site_form_unionlog
         |WHERE
         |  $selectCondition2
       """.stripMargin
    println(sqlRequest3)
    val cvr3Data = spark.sql(sqlRequest3).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .select("searchid", "ideaid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr1", "iscvr2", "iscvr3")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3"))))
      .select("searchid", "ideaid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr")
      .na.fill(0, Seq("iscvr"))

    resultDF


  }

  def calculateByIdeaid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
//    预测cvr
//    acb
//    auc
//    q_factor
//    suggest_cpa
    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
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
         |GROUP BY ideaid, userid, conversion_goal
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
         |GROUP BY ideaid, userid, conversion_goal
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def calculateAUCbyIdeaid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("ideaid", "userid", "conversion_goal").distinct()
    val aucList = new mutable.ListBuffer[(Int, Int, Int, Double)]()
    var cnt = 0

    for (row <- key.collect()) {
      val ideaid = row.getAs[Int]("ideaid")
      val userid = row.getAs[Int]("userid")
      val conversion_goal = row.getAs[Int]("conversion_goal")
      val selectCondition = s"ideaid=$ideaid and userid=$userid and conversion_goal=$conversion_goal"
      println(selectCondition)
      val singleData = data
        .withColumn("score", col("exp_cvr"))
        .withColumn("label", col("iscvr"))
        .filter(selectCondition)
      val scoreAndLabel = singleData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Double]("score").toDouble, x.getAs[Int]("label").toDouble))
      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        println(s"### result is $aucROC, cnt=$cnt ###")
        aucList.append((ideaid, userid, conversion_goal, aucROC))
      }
      cnt += 1
    }

    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("ideaid", "userid", "conversion_goal", "auc")

    resultDF
  }

  def calculateAUCbyConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("conversion_goal").distinct()
    val aucList = new mutable.ListBuffer[(Int, Double)]()
    var cnt = 0

    for (row <- key.collect()) {
      val conversion_goal = row.getAs[Int]("conversion_goal")
      val selectCondition = s"conversion_goal=$conversion_goal"
      println(selectCondition)
      val singleData = data
        .withColumn("score", col("exp_cvr"))
        .withColumn("label", col("iscvr"))
        .filter(selectCondition)
      val scoreAndLabel = singleData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Double]("score").toDouble, x.getAs[Int]("label").toDouble))
      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        println(s"### result is $aucROC, cnt=$cnt ###")
        aucList.append((conversion_goal, aucROC))
      }
      cnt += 1
    }

    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("conversion_goal", "auc")

    resultDF

  }

}
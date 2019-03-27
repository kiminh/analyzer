package com.cpc.spark.OcpcProtoType.report_qtt

import com.cpc.spark.ocpcV3.utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object OcpcHourlyAucReport {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyAucReport: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(media, date, hour, spark)

    // 详情表数据
    val unitData1 = calculateByUnitid(rawData, date, hour, spark)
    val unitData2 = calculateAUCbyUnitid(rawData, date, hour, spark)
    val unitData = unitData1
      .join(unitData2, Seq("unitid", "userid", "conversion_goal"), "left_outer")
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "pre_cvr", "cast(post_cvr as double) post_cvr", "q_factor", "cpagiven", "cast(cpareal as double) cpareal", "cast(acp as double) acp", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    unitData.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190226")
    unitData
      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_detail_hourly")

    // 汇总表数据
    val conversionData1 = calculateByConversionGoal(rawData, date, hour, spark)
    val conversionData2 = calculateAUCbyConversionGoal(rawData, date, hour, spark)
    val conversionData = conversionData1
      .join(conversionData2, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    conversionData.write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly20190226")
    conversionData
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_summary_hourly")


  }

  def getOcpcLog(media: String, date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)
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
         |    exp_cvr,
         |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |    cast(ocpc_log_dict['dynamicbid'] as double) as bid,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |    price
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |and is_ocpc=1
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
       """.stripMargin
    println(sqlRequest)
    val ctrData = spark.sql(sqlRequest).filter(s"is_hidden != 1")

    // cvr1Data
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr1'
       """.stripMargin
    println(sqlRequest1)
    val cvr1Data = spark.sql(sqlRequest1).distinct()

    // cvr2Data
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr2'
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
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest3)
    val cvr3Data = spark.sql(sqlRequest3).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr1", "iscvr2", "iscvr3")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3"))))
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


  def calculateAUCbyUnitid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("unitid", "userid", "conversion_goal").distinct()
    import spark.implicits._

    val newData = data
      .withColumn("identifier", concat_ws("-", col("unitid"), col("userid"), col("conversion_goal")))
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .selectExpr("identifier", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "identifier")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val identifierList = identifier.trim.split("-")
      val ideaid = identifierList(0).toInt
      val userid = identifierList(1).toInt
      val conversionGoal = identifierList(2).toInt
      val auc = row.getAs[Double]("auc")
      (ideaid, userid, conversionGoal, auc)
    })
    val resultDF = resultRDD.toDF("unitid", "userid", "conversion_goal", "auc")
    resultDF
  }

//  def calculateAUCbyConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
//    import spark.implicits._
//
//    val newData = data
//      .withColumn("score", col("exp_cvr") * 1000000)
//      .withColumn("label", col("iscvr"))
//      .selectExpr("cast(conversion_goal as string) conversion_goal", "cast(score as int) score", "label")
//      .coalesce(400)
//
//    val result = utils.getGauc(spark, newData, "conversion_goal")
//    val resultRDD = result.rdd.map(row => {
//      val identifier = row.getAs[String]("name").toInt
//      val auc = row.getAs[Double]("auc")
//      (identifier, auc)
//    })
//    val resultDF = resultRDD.toDF("conversion_goal", "auc")
//    resultDF
//
//  }
//
//  def calculateAUCbyUnitid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
//    val key = data.select("ideaid", "userid", "conversion_goal").distinct()
//    val aucList = new mutable.ListBuffer[(Int, Int, Int, Double)]()
//    var cnt = 0
//
//    for (row <- key.collect()) {
//      val ideaid = row.getAs[Int]("ideaid")
//      val userid = row.getAs[Int]("userid")
//      val conversion_goal = row.getAs[Int]("conversion_goal")
//      val selectCondition = s"ideaid=$ideaid and userid=$userid and conversion_goal=$conversion_goal"
//      println(selectCondition)
//      val singleData = data
//        .withColumn("score", col("exp_cvr"))
//        .withColumn("label", col("iscvr"))
//        .filter(selectCondition)
//      val scoreAndLabel = singleData
//        .select("score", "label")
//        .rdd
//        .map(x=>(x.getAs[Double]("score").toDouble, x.getAs[Int]("label").toDouble))
//      val scoreAndLabelNum = scoreAndLabel.count()
//      if (scoreAndLabelNum > 0) {
//        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
//        val aucROC = metrics.areaUnderROC
//        println(s"### result is $aucROC, cnt=$cnt ###")
//        aucList.append((ideaid, userid, conversion_goal, aucROC))
//      }
//      cnt += 1
//    }
//
//    val resultDF = spark
//      .createDataFrame(aucList)
//      .toDF("ideaid", "userid", "conversion_goal", "auc")
//
//    resultDF
//  }
//
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
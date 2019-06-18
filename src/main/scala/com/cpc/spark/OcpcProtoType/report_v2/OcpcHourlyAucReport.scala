package com.cpc.spark.OcpcProtoType.report_v2

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
    val conversionGoal = args(4).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media, conversionGoal=$conversionGoal")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyAucReport: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(media, conversionGoal, date, hour, spark)

    // 详情表数据
    val versionUnit = version + "_unitid"
    val unitData = calculateAUCbyUnitid(rawData, date, hour, spark)

    unitData.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190226a")
//    unitData
//      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_detail_hourly")


    // 详情表数据
    val versionUserid = version + "_userid"
    val userData = calculateAUCbyUserid(rawData, date, hour, spark)

    userData.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190226b")
//    userData
//      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_detail_hourly")

    // 汇总表数据
    val conversionData = calculateAUCbyConversionGoal(rawData, date, hour, spark)

    conversionData.write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly20190226")
//    conversionData
//      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_summary_hourly")


  }

  def getOcpcLog(media: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)
    val selectCondition = s"`date` = '$date' and `hour` <= '$hour'"

    // ctrData
    val sqlRequest1 =
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
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |    price
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |and is_ocpc=1
         |and media_appsid in ('80000001', '80000002', '80002819')
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
         |and conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1)

    // cvrData
    val cvrType = "cvr" + conversionGoal.toString
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvrData, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "price", "iscvr", "is_hidden")
      .na.fill(0, Seq("iscvr"))

    resultDF


  }

  def calculateAUCbyUserid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("userid", "is_hidden").distinct()
    import spark.implicits._

    val newData = data
      .withColumn("identifier", concat_ws("-", col("userid"), col("is_hidden")))
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .selectExpr("identifier", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "identifier")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val identifierList = identifier.trim.split("-")
      val userid = identifierList(0).toInt
      val isHidden = identifierList(1).toInt
      val auc = row.getAs[Double]("auc")
      (userid, isHidden, auc)
    })
    val resultDF = resultRDD.toDF("userid", "is_hidden", "auc")
    resultDF
  }


  def calculateAUCbyUnitid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("unitid", "userid", "is_hidden").distinct()
    import spark.implicits._

    val newData = data
      .withColumn("identifier", concat_ws("-", col("unitid"), col("userid"), col("is_hidden")))
      .withColumn("score", col("exp_cvr") * 1000000)
      .withColumn("label", col("iscvr"))
      .selectExpr("identifier", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "identifier")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val identifierList = identifier.trim.split("-")
      val unitid = identifierList(0).toInt
      val userid = identifierList(1).toInt
      val isHidden = identifierList(2).toInt
      val auc = row.getAs[Double]("auc")
      (unitid, userid, isHidden, auc)
    })
    val resultDF = resultRDD.toDF("unitid", "userid", "is_hidden", "auc")
    resultDF
  }

  def calculateAUCbyConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data
      .select("is_hidden")
      .distinct()
    val aucList = new mutable.ListBuffer[(Int, Double)]()
    var cnt = 0

    for (row <- key.collect()) {
      val isHidden = row.getAs[Int]("is_hidden")
      val selectCondition = s"is_hidden=$isHidden"
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
        aucList.append((isHidden, aucROC))
      }
      cnt += 1
    }

    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("is_hidden", "auc")

    resultDF

  }

}
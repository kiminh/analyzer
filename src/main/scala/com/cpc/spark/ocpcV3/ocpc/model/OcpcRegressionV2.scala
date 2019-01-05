package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfSqrt
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession, DataFrame}

import scala.collection.mutable

object OcpcRegressionV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    // bash: 2019-01-02 12 72 version2 novel
    val date = args(0).toString
    val hour = args(1).toString
    val hourCnt = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString
    var mediaSelection = s"media_appsid in ('80000001', '80000002')"
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else {
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    }

    // 中间表
    val middleData = getMiddleData(mediaSelection, version, hourCnt, date, hour, spark)
    val tablename = "dl_cpc.ocpc_regression_middle_hourly"
    val result = middleData
      .withColumn("conversion_goal", lit(1))
      .selectExpr("cast(identifier as string) identifier", "k_ratio", "cpagiven", "cpa", "ratio", "click_cnt", "cvr_cnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//    result.write.mode("overwrite").saveAsTable(tablename)
    result.write.mode("overwrite").insertInto(tablename)

    // 结果表
    val kvalue = getKWithRatio(middleData, date, hour, spark)
    val resultDF = kvalue
      .withColumn("conversion_goal", lit(1))
      .select("identifier", "k_ratio", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_regression_hourly")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_k_regression_hourly")


  }

  def getMiddleData(mediaSelection: String, version: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 获取并关联数据
    val ctrData = getCtrData(mediaSelection, hourCnt, date, hour, spark)
    val cvrData = getCvr1Data(mediaSelection, hourCnt, date, hour, spark)
    val rawData = ctrData
      .join(cvrData, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("label"))
      .select("identifier", "kvalue", "cpagiven", "isclick", "price", "label")

    rawData.createOrReplaceTempView("base_table")
    val sqlRequest =
      s"""
         |select
         |  identifier,
         |  round(kvalue * 100.0 / 5) as k_ratio,
         |  cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label,0)) as cpa,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label,0))/cpagiven as ratio,
         |  sum(isclick) click_cnt,
         |  sum(COALESCE(label,0)) cvr_cnt
         |from
         |  base_table
         |group by
         |  identifier,
         |  round(kvalue * 100.0 / 5),
         |  cpagiven
      """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF

  }

  def getCtrData(mediaSelection: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  cast(unitid as string) as identifier,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['kvalue'] as double) as kvalue
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition
         |and
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def getCvr1Data(mediaSelection: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label2 as label
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  $selectCondition
         |AND
         |  label_type!=12
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }



  def getKWithRatio(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {

    val res = baseData
      .filter(s"ratio is not null")
      .withColumn("str", concat_ws(" ", col(s"k_ratio"), col("ratio"), col("click_cnt")))
      .groupBy("identifier")
      .agg(collect_set("str").as("liststr"))
      .select("identifier", "liststr").collect()
//    rawData.write.mode("overwrite").saveAsTable("test.ocpc_check_regression20190103")

//    val res = rawData.collect()


    var resList = new mutable.ListBuffer[(String, Double)]()
    for (row <- res) {
      val identifier = row(0).toString
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val targetK = 0.95
      val k = (targetK - coffList(0)) / coffList(1)
      val realk: Double = k * 5.0 / 100.0
      println("identifier " + identifier, "coff " + coffList, "target k: " + k, "realk: " + realk, "targetK: " + targetK)
      if (coffList(1)>0 && realk > 0) {
        resList.append((identifier, realk))
      }
    }

    val data = spark.createDataFrame(resList)
      .toDF("identifier", "k_ratio")
    data
  }

  def fitPoints(pointsWithCount: List[(Double, Double, Int)]): List[Double] = {
    var obs: WeightedObservedPoints = new WeightedObservedPoints();
    var count = 0
    for ((x, y, n) <- pointsWithCount) {
      for (i <- 1 to n) {
        obs.add(x, y);
      }
      count = count + n
      println("sample", x, y, n)
    }

    for (i <- 0 to count / 10) {
      obs.add(0.0, 0.0);
    }

    // Instantiate a third-degree polynomial fitter.
    var fitter: PolynomialCurveFitter = PolynomialCurveFitter.create(1);

    var res = mutable.ListBuffer[Double]()
    // Retrieve fitted parameters (coefficients of the polynomial function).
    for (c <- fitter.fit(obs.toList)) {
      res.append(c)
    }
    res.toList
  }



}


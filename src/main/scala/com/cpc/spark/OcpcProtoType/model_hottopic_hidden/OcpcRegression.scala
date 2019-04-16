package com.cpc.spark.OcpcProtoType.model_hottopic_hidden

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable

object OcpcRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 72 1 hottopic_test hottopic
    val date = args(0).toString
    val hour = args(1).toString
    val hourCnt = args(2).toInt
    val conversionGoal = args(3).toInt
    val version = args(4).toString
    val media = args(5).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, hourCnt=$hourCnt, conversionGoal=$conversionGoal, version=$version, media=$media")
    var mediaSelection = ""
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else if (media == "novel") {
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid = '80002819'"
    }

    val result = calcualteKwithRegression(mediaSelection, conversionGoal, version, hourCnt, date, hour, spark)


    val resultDF = result
      .withColumn("kvalue", col("k_ratio"))
      .select("identifier", "kvalue")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("method", lit("regression"))

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_regression_hourly")
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_k_model_hourly")


  }

  def calcualteKwithRegression(mediaSelection: String, conversionGoal: Int, version: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 中间表
    val middleData = getMiddleData(mediaSelection, conversionGoal, version, hourCnt, date, hour, spark)
    val tablename = "dl_cpc.ocpc_regression_middle_hourly_v2"
    val result = middleData
      .selectExpr("cast(identifier as string) identifier", "k_ratio", "cpagiven", "cpa", "ratio", "click_cnt", "cvr_cnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    result.write.mode("overwrite").saveAsTable(tablename)
    result
      .repartition(10).write.mode("overwrite").insertInto(tablename)

    // 结果表
    val kvalue = getKWithRatio(middleData, date, hour, spark)

    val resultDF = kvalue
      .withColumn("conversion_goal", lit(conversionGoal))
      .select("identifier", "k_ratio", "conversion_goal")

    resultDF
  }

  def getMiddleData(mediaSelection: String, conversionGoal: Int, version: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 获取并关联数据
    val ctrData = getCtrData(mediaSelection, hourCnt, date, hour, spark)
    val cvrData = getCvrData(mediaSelection, conversionGoal, hourCnt, date, hour, spark)
//    val cvrData = getCvr1Data(mediaSelection, hourCnt, date, hour, spark)
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
    val resultDF = spark
        .sql(sqlRequest)
        .withColumn("conversion_goal", lit(conversionGoal))

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
         |  cast(unitid as string) identifier,
         |  adclass,
         |  isshow,
         |  isclick,
         |  price,
         |  ocpc_log_dict,
         |  cast(ocpc_log_dict['kvalue'] as double) as kvalue,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  (ocpc_log_dict['cpcBid']=0 or exptags not like "%cpcBid%")
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def getCvrData(mediaSelection: String, conversionGoal: Int, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // cvr 分区
    var cvrGoal = ""
    if (conversionGoal == 1) {
      cvrGoal = "cvr1"
    } else if (conversionGoal == 2) {
      cvrGoal = "cvr2"
    } else {
      cvrGoal = "cvr3"
    }

    // 时间分区
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
    val selectCondition = s"`date` >= '$date1'"
//    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 抽取数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  (cvr_goal = '$cvrGoal')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)

    resultDF
  }

//  def getCvr1Data(mediaSelection: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
//    val newDate = date + " " + hour
//    val today = dateConverter.parse(newDate)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.HOUR, -hourCnt)
//    val yesterday = calendar.getTime
//    val tmpDate = dateConverter.format(yesterday)
//    val tmpDateValue = tmpDate.split(" ")
//    val date1 = tmpDateValue(0)
//    val hour1 = tmpDateValue(1)
//    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
//
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  searchid,
//         |  label2 as label
//         |FROM
//         |  dl_cpc.ml_cvr_feature_v1
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  label_type!=12
//         |AND
//         |  $mediaSelection
//       """.stripMargin
//    println(sqlRequest)
//    val resultDF = spark.sql(sqlRequest)
//
//    resultDF
//  }



  def getKWithRatio(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {

    val res = baseData
      .filter(s"ratio is not null")
      .withColumn("str", concat_ws(" ", col(s"k_ratio"), col("ratio"), col("click_cnt")))
      .groupBy("identifier")
      .agg(collect_set("str").as("liststr"))
      .select("identifier", "liststr").collect()



    var resList = new mutable.ListBuffer[(String, Double)]()
    for (row <- res) {
      val identifier = row(0).toString
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      // todo
      val targetK = 0.98
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



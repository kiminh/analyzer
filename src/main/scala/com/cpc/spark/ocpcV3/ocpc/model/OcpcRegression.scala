package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfSqrt
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

object OcpcRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    calculateK(date, hour, spark)
  }

  def calculateK(date: String, hour: String, spark: SparkSession) = {
//    val datehourlist = scala.collection.mutable.ListBuffer[String]()
//    val datehourlist2 = scala.collection.mutable.ListBuffer[String]()
//    val cal = Calendar.getInstance()
//    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
//    for (t <- 0 to 72) {
//      if (t > 0) {
//        cal.add(Calendar.HOUR, -1)
//      }
//      val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val dd = sf.format(cal.getTime())
//      val d1 = dd.substring(0, 10)
//      val h1 = dd.substring(11, 13)
//      val datecond = s"`date` = '$d1' and hour = '$h1'"
//      val datecond2 = s"`dt` = '$d1' and hour = '$h1'"
//      datehourlist += datecond
//      datehourlist2 += datecond2
//    }

    // 取历史数据
    val conf = ConfigFactory.load("ocpc")
    val hourCnt = conf.getInt("ocpc_all.ocpc_regression.hour_cnt")
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
//
//    val dtCondition = "(%s)".format(datehourlist.mkString(" or "))
//    val dtCondition2 = "(%s)".format(datehourlist2.mkString(" or "))

    val selectorID = conf.getString("ocpc_all.ocpc_regression.select_keys")
    val identifier = conf.getString("ocpc_all.ocpc_regression.identifier")

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  $selectorID,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['kvalue'] as double) as kvalue
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition
         |and
         |  media_appsid in ('80000001', '80000002')
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label2 as label1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  $selectCondition
         |AND
         |  label_type!=12
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  label as label2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val cvr2Data = spark.sql(sqlRequest3)

    val base = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")

    base.createOrReplaceTempView("base_table")

    val sqlRequest4 =
      s"""
         |select
         |  $selectorID,
         |  round(kvalue * 100.0 / 5) as k_ratio,
         |  cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0)) as cpa,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0))/cpagiven as ratio,
         |  sum(isclick) click_cnt,
         |  sum(COALESCE(label1,0)) cvr_cnt
         |from
         |  base_table
         |group by $selectorID,
         |  round(kvalue * 100.0 / 5),
         |  cpagiven
      """.stripMargin

    println(sqlRequest4)
    val data1 = spark
      .sql(sqlRequest4)
      .withColumn("conversion_goal", lit(1))

    val sqlRequest5 =
      s"""
         |select
         |  $selectorID,
         |  round(kvalue * 100.0 / 5) as k_ratio,
         |  cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/cpagiven as ratio,
         |  sum(isclick) click_cnt,
         |  sum(COALESCE(label2,0)) cvr_cnt
         |from
         |  base_table
         |group by $selectorID,
         |  round(kvalue * 100.0 / 5),
         |  cpagiven
      """.stripMargin

    println(sqlRequest5)
    val data2 = spark
      .sql(sqlRequest5)
      .withColumn("conversion_goal", lit(2))

    val tablename = "dl_cpc.ocpc_regression_middle_hourly"
    val result = data1
      .union(data2)
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "k_ratio", "cpagiven", "cpa", "ratio", "click_cnt", "cvr_cnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("v1"))

//    result.write.mode("overwrite").saveAsTable(tablename)
    result
      .repartition(10).write.mode("overwrite").insertInto(tablename)

    val ratio1Data = getKWithRatioType(spark, tablename, 1, date, hour)
    val ratio2Data = getKWithRatioType(spark, tablename, 2, date, hour)

    val res = ratio1Data
      .union(ratio2Data)
      .select("identifier", "k_ratio", "conversion_goal", "date", "hour")
      .withColumn("version", lit("v1"))
//    res.write.mode("overwrite").saveAsTable("test.ocpc_k_regression_hourly")
    res
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_k_regression_hourly")
  }

  def getKWithRatioType(spark: SparkSession, tablename: String, conversionGoal: Int, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and version = 'v1' and conversion_goal = $conversionGoal and ratio is not null"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col(s"k_ratio"), col("ratio"), col("click_cnt")))
      .groupBy("identifier")
      .agg(collect_set("str").as("liststr"))
      .select("identifier", "liststr").collect()


    var resList = new mutable.ListBuffer[(String, Double, Int, String, String)]()
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
        resList.append((identifier, realk, conversionGoal, date, hour))
      }
    }

    val data = spark.createDataFrame(resList)
      .toDF("identifier", "k_ratio", "conversion_goal", "date", "hour")
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

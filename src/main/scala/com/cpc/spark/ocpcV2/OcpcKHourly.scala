package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, lit}
import org.apache.spark.ml.feature.OneHotEncoder

import scala.collection.mutable

object OcpcKHourly {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val dayCnt = 7

    // TODO 分段拟合
    getK(date, hour, dayCnt, spark)
  }

  def getK(date: String, hour: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val start_date = calendar.getTime
    val date1 = dateConverter.format(start_date)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
    val selectCondition2 = getTimeRangeSql3(date1, hour, date, hour)

    val sqlRequest1 =
      s"""
         |select
         |  ideaid,
         |  hour,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5) as k_ratio2,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5) as k_ratio3,
         |  ocpc_log_dict['cpagiven'] as cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0)) as cpa3,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0))/ocpc_log_dict['cpagiven'] as ratio3,
         |  sum(isclick) clickCnt,
         |  sum(COALESCE(label2,0)) cvr2Cnt,
         |  sum(COALESCE(label3,0)) cvr3Cnt
         |from
         |  (select * from dl_cpc.ocpc_unionlog where $selectCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1) a
         |  left outer join
         |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $selectCondition) b on a.searchid = b.searchid
         |  left outer join
         |  (select searchid, iscvr as label3 from dl_cpc.cpc_api_union_log where $selectCondition) c on a.searchid = c.searchid
         |group by
         |  ideaid,
         |  hour,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5) ,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5),
         |  ocpc_log_dict['cpagiven']
      """.stripMargin

    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    val encoder = new OneHotEncoder()


    val tablename = "dl_cpc.cpc_ocpc_v2_regression_timespan_middle"
    spark.sql(sqlRequest1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write.mode("overwrite").insertInto(tablename)

    val ratio2Data = getKWithRatioType(spark, tablename, "ratio2", date, hour)
    val ratio3Data = getKWithRatioType(spark, tablename, "ratio3", date, hour)

    val res = ratio2Data.join(ratio3Data, Seq("ideaid", "time_span", "date", "hour"), "outer")
      .select("ideaid", "time_span", "k_ratio2", "k_ratio3", "date", "hour")
    res.write.mode("overwrite").insertInto("dl_cpc.ocpc_v2_k_timespan_regression")



  }


  def getKWithRatioType(spark: SparkSession, tablename: String, ratioType: String, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and $ratioType is not null"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col(s"k_$ratioType"), col(s"$ratioType"), col("clickCnt")))
      .groupBy("ideaid", "time_span")
      .agg(collect_set("str").as("liststr"))
      .select("ideaid", "time_span", "liststr").collect()

    val targetK = 0.95
    var resList = new mutable.ListBuffer[(String, String, Double, String, String)]()
    for (row <- res) {
      val ideaid = row(0).toString
      val timeSpan = row(1).toString
      val pointList = row(2).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val k = (targetK - coffList(0)) / coffList(1)
      val realk: Double = k * 5.0 / 100.0
      println("ideaid " + ideaid, "time_span " + timeSpan, "coff " + coffList, "target k: " + k, "realk: " + realk)
      if (realk > 0) {
        resList.append((ideaid, timeSpan, realk, date, hour))
      }
    }
    val data = spark.createDataFrame(resList)
      .toDF("ideaid", "time_span", s"k_$ratioType", "date", "hour")
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

    for (i <- 0 to count / 5) {
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
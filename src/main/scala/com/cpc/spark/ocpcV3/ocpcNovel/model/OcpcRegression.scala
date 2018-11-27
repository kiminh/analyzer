package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import com.cpc.spark.udfs.Udfs_wj.udfSqrt
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
    val datehourlist = scala.collection.mutable.ListBuffer[String]()
    val datehourlist2 = scala.collection.mutable.ListBuffer[String]()
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
    for (t <- 0 to 72) {
      if (t > 0) {
        cal.add(Calendar.HOUR, -1)
      }
      val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dd = sf.format(cal.getTime())
      val d1 = dd.substring(0, 10)
      val h1 = dd.substring(11, 13)
      val datecond = s"`date` = '$d1' and hour = '$h1'"
      val datecond2 = s"`dt` = '$d1' and hour = '$h1'"
      datehourlist += datecond
      datehourlist2 += datecond2
    }

    val dtCondition = "(%s)".format(datehourlist.mkString(" or "))
    val dtCondition2 = "(%s)".format(datehourlist2.mkString(" or "))



    // TODO  替换成弘扬的表
    val statSql =
      s"""
         |select
         |  unitid,
         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio1,
         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio2,
         |  ocpc_log_dict['cpagiven'] as cpagiven,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0)) as cpa1,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0))/ocpc_log_dict['cpagiven'] as ratio1,
         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
         |  sum(isclick) clickCnt,
         |  sum(COALESCE(label1,0)) cvr1Cnt,
         |  sum(COALESCE(label2,0)) cvr2Cnt
         |from
         |  (select unitid, isclick, price, ocpc_log_dict, iscvr1 as label1, iscvr2 as label2 from dl_cpc.ocpcv3_unionlog_label_hourly where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1 and media_appsid in ("80001098","80001292")) a
         |group by unitid,
         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5),
         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5),
         |  ocpc_log_dict['cpagiven']
      """.stripMargin

    println(statSql)

    val tablename = "test.ocpc_v3_novel_regression_middle"
    val rawData = spark.sql(statSql)


    val data = rawData
      .select("unitid", "k_ratio1", "k_ratio2", "cpagiven", "cpa1", "cpa2", "ratio1", "ratio2", "clickCnt", "cvr1Cnt", "cvr2Cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    data.write.mode("overwrite").saveAsTable(tablename)
//    data.write.mode("overwrite").insertInto(tablename)

    val ratio1Data = getKWithRatioType(spark, tablename, "ratio1", date, hour)
    val ratio2Data = getKWithRatioType(spark, tablename, "ratio2", date, hour)

    val res = ratio1Data.join(ratio2Data, Seq("unitid", "date", "hour"), "outer")
      .select("unitid", "k_ratio1", "k_ratio2", "date", "hour")
    res.write.mode("overwrite").saveAsTable("test.ocpc_v3_novel_k_regression")
//    res.write.mode("overwrite").insertInto("dl_cpc.ocpc_v2_k")
  }

  def getKWithRatioType(spark: SparkSession, tablename: String, ratioType: String, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and $ratioType is not null"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col(s"k_$ratioType"), col(s"$ratioType"), col("clickCnt")))
      .groupBy("unitid")
      .agg(collect_set("str").as("liststr"))
      .select("unitid", "liststr").collect()

    val targetK = 0.95
    var resList = new mutable.ListBuffer[(String, Double, String, String)]()
    for (row <- res) {
      val unitid = row(0).toString
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val k = (targetK - coffList(0)) / coffList(1)
      val realk: Double = k * 5.0 / 100.0
      println("unitid " + unitid, "coff " + coffList, "target k: " + k, "realk: " + realk, "targetK: " + targetK)
      if (coffList(1)>0 && realk > 0) {
        resList.append((unitid, realk, date, hour))
      }
    }
    val data = spark.createDataFrame(resList)
      .toDF("unitid", s"k_$ratioType", "date", "hour")
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
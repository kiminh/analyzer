package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import com.cpc.spark.udfs.Udfs_wj.udfSqrt
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

object OcpcRegressionV2 {
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

    val statSql =
      s"""
         |select
         |  a.unitid,
         |  round(a.ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio1,
         |  round(a.ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio2,
         |  a.ocpc_log_dict['cpagiven'] as cpagiven,
         |  sum(if(a.isclick=1,price,0))/sum(COALESCE(a.label1,0)) as cpa1,
         |  sum(if(a.isclick=1,price,0))/sum(COALESCE(b.label2,0)) as cpa2,
         |  sum(if(a.isclick=1,price,0))/sum(COALESCE(a.label1,0))/a.ocpc_log_dict['cpagiven'] as ratio1,
         |  sum(if(a.isclick=1,price,0))/sum(COALESCE(b.label2,0))/a.ocpc_log_dict['cpagiven'] as ratio2,
         |  sum(a.isclick) clickCnt,
         |  sum(COALESCE(a.label1,0)) cvr1Cnt,
         |  sum(COALESCE(b.label2,0)) cvr2Cnt
         |from
         |  (select searchid, unitid, isclick, price, ocpc_log_dict, iscvr1 as label1 from dl_cpc.ocpcv3_unionlog_label_hourly where $dtCondition and ocpc_log_dict['kvalue'] is not null and isclick=1 and media_appsid in ("80001098","80001292")) as a
         |LEFT JOIN
         |  (select searchid, label as label2 from dl_cpc.ml_cvr_feature_v2 where $dtCondition and label=1 group by searchid, label) as b
         |on
         |  a.searchid=b.searchid
         |group by a.unitid,
         |  round(a.ocpc_log_dict['kvalue'] * 100.0 / 5),
         |  round(a.ocpc_log_dict['kvalue'] * 100.0 / 5),
         |  a.ocpc_log_dict['cpagiven']
      """.stripMargin

    println(statSql)

    val tablename = "dl_cpc.ocpc_v3_novel_regression_middle_v2"
    val rawData = spark.sql(statSql)


    val data = rawData
      .select("unitid", "k_ratio1", "k_ratio2", "cpagiven", "cpa1", "cpa2", "ratio1", "ratio2", "clickCnt", "cvr1Cnt", "cvr2Cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable(tablename)
    data
      .repartition(10).write.mode("overwrite").insertInto(tablename)

    val ratio1Data = getKWithRatioType(spark, tablename, "ratio1", date, hour)
    val ratio2Data = getKWithRatioType(spark, tablename, "ratio2", date, hour)

    val res = ratio1Data.join(ratio2Data, Seq("unitid", "date", "hour"), "outer")
      .select("unitid", "k_ratio1", "k_ratio2", "date", "hour")
//    res.write.mode("overwrite").saveAsTable("test.ocpc_v3_novel_k_regression_v2")
    res
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_v3_novel_k_regression_v2")
  }

  def getKWithRatioType(spark: SparkSession, tablename: String, ratioType: String, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and $ratioType is not null"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col(s"k_$ratioType"), col(s"$ratioType"), col("clickCnt")))
      .groupBy("unitid")
      .agg(collect_set("str").as("liststr"))
      .select("unitid", "liststr").collect()


    val cpaSrcMap = getCPAsrcMap(date, hour, spark)
    var resList = new mutable.ListBuffer[(String, Double, String, String)]()
//    var testList = new mutable.ListBuffer[(String, Double, Double, Double, Double, Double)]()
    for (row <- res) {
      val unitid = row(0).toString
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      // TODO 根据cpa_src决定targetK
      val targetK = getTargetK(unitid, cpaSrcMap, date, hour, spark)
      val k = (targetK - coffList(0)) / coffList(1)
      val realk: Double = k * 5.0 / 100.0
      println("unitid " + unitid, "coff " + coffList, "target k: " + k, "realk: " + realk, "targetK: " + targetK)
//      testList.append((unitid, coffList(0), coffList(1), k, realk, targetK))
      if (coffList(1)>0 && realk > 0) {
        resList.append((unitid, realk, date, hour))
      }
    }
//    // TODO
//    val testData = spark.createDataFrame(testList).toDF("unitid", "coff0", "coff1", "k", "realk", "target_k")
//    testData.write.mode("overwrite").saveAsTable("test.ocpcv3_regression_check_target_" + ratioType)


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

  def getCPAsrcMap(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpcv3_cpa_history_v2_final_middle")
      .where(s"`date`='$date' and `hour`='$hour'")

    var cpaMap = mutable.LinkedHashMap[String, String]()
    for(row <- data.collect()) {
      val unitid = row.getAs[Int]("unitid").toString
      val cpaSrc = row.getAs[String]("cpa_src")
      cpaMap += (unitid -> cpaSrc)
    }
    cpaMap
  }

  def getTargetK(unitid: String, cpaSRC: mutable.LinkedHashMap[String, String], date: String, hour: String, spark: SparkSession) = {
    val cpasrc = cpaSRC.getOrElse(unitid, "qtt")
    var targetK = 1.8
    if (cpasrc == "novel") {
      targetK = 1.0
    } else {
      targetK = 1.8
    }
    targetK
  }


}

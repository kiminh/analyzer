package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._

import com.cpc.spark.udfs.Udfs_wj._

object OcpcKv2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

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
         |  ideaid,
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
         |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1) a
         |  left outer join
         |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $dtCondition) b on a.searchid = b.searchid
         |  left outer join
         |  (select searchid, label as label3 from dl_cpc.ml_cvr_feature_v2 where $dtCondition and label=1 group by searchid, label) c on a.searchid = c.searchid
         |group by ideaid,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5),
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5),
         |  ocpc_log_dict['cpagiven']
      """.stripMargin

    println(statSql)

    val tablename = "dl_cpc.ocpc_regression_middle"
    val data = spark
      .sql(statSql)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable(tablename)
    data.write.mode("overwrite").insertInto(tablename)

    val ratio2Data = getKWithRatioType(spark, tablename, "ratio2", date, hour)
    val ratio3Data = getKWithRatioType(spark, tablename, "ratio3", date, hour)

    val res = ratio2Data.join(ratio3Data, Seq("ideaid", "date", "hour"), "outer")
      .select("ideaid", "k_ratio2", "k_ratio3", "date", "hour")
//    res.write.mode("overwrite").saveAsTable("test.ocpc_regression_k")
    res.write.mode("overwrite").insertInto("dl_cpc.ocpc_regression_k")

  }

  def getKWithRatioType(spark: SparkSession, tablename: String, ratioType: String, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and $ratioType is not null"
    println("getKWithRatioType", condition)
    val rawData = spark
      .table(tablename)
      .where(condition)
      .withColumn("ratio_type", lit(ratioType))
      .withColumn("cvrCnt", when(col("ratio_type")==="ratio2", col("cvr2Cnt")).otherwise(col("cvr3Cnt")))
    rawData.show(10)

    val res = rawData
      .withColumn("str", concat_ws(" ", col(s"k_$ratioType"), col(s"$ratioType"), col("cvrCnt")))
      .groupBy("ideaid")
      .agg(collect_set("str").as("liststr"))
      .select("ideaid", "liststr").collect()

    val cpaMap = getCPAratio2(date, hour, spark)
    var resList = new mutable.ListBuffer[(String, Double, String, String)]()
    for (row <- res) {
      val ideaid = row(0).toString
      val pointList = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map(x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      })
      val coffList = fitPoints(pointList.toList)
      val targetK = getTargetK2(cpaMap, hour, ideaid, spark)
      val cpaRatio = cpaMap.getOrElse(ideaid, 0.0)

      val k = (targetK - coffList(0)) / coffList(1)
      val realk: Double = k * 5.0 / 100.0
      println("ideaid " + ideaid, "coff " + coffList, "target k: " + k, "realk: " + realk, "targetK: " + targetK, "cpaRatio: " + cpaRatio)
      if (coffList(1)>0 && realk > 0) {
        resList.append((ideaid, realk, date, hour))
      }
    }
    val data = spark.createDataFrame(resList)
      .toDF("ideaid", s"k_$ratioType", "date", "hour")
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

  def getCPAratio2(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    // cost数据
    val rawData1 = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date' and `hour` <= '$hour'")
      .filter("isclick=1 and ocpc_log_dict['kvalue'] is not null")

    val costData = rawData1
      .groupBy("ideaid")
      .agg(
        sum(col("price")).alias("cost"),
        avg(col("cpa_given")).alias("cpa_given"))

    // cvr2数据
    val rawData2 = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(s"`date`='$date' and `hour` <= '$hour'")
      .filter("label2=1")
      .withColumn("label", col("label2"))
      .select("ideaid", "label", "searchid")
      .distinct()

    val cvr2Data = rawData2
      .groupBy("ideaid")
      .agg(sum(col("label")).alias("cvr2_cnt"))
      .select("ideaid", "cvr2_cnt")

    // cvr3数据
    val rawData3 = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(s"`date`='$date' and `hour` <= '$hour'")
      .filter("label=1")
      .select("ideaid", "label", "searchid")
      .distinct()

    val cvr3Data = rawData3
      .groupBy("ideaid")
      .agg(sum(col("label")).alias("cvr3_cnt"))
      .select("ideaid", "cvr3_cnt")

    // 读取ideaid的转化目标
    val ideaids = spark
      .table("test.ocpc_idea_update_time_" + hour)
      .select("ideaid", "conversion_goal")
      .distinct()

    val resultDF = ideaids
      .join(costData, Seq("ideaid"), "left_outer")
      .join(cvr2Data, Seq("ideaid"), "left_outer")
      .join(cvr3Data, Seq("ideaid"), "left_outer")
      .select("ideaid", "cpa_given", "cost", "cvr2_cnt", "cvr3_cnt", "conversion_goal")
      .withColumn("cpa2_real", col("cost") * 1.0 / col("cvr2_cnt"))
      .withColumn("cpa3_real", col("cost") * 1.0 / col("cvr3_cnt"))
      .withColumn("cpa2_ratio", col("cpa_given") * 1.0 / col("cpa2_real"))
      .withColumn("cpa3_ratio", col("cpa_given") * 1.0 / col("cpa3_real"))
      .withColumn("cpa_ratio", when(col("conversion_goal") === 2, col("cpa3_ratio")).otherwise(col("cpa2_ratio")))
      .filter("cpa_ratio is not null")


    println("cpa ratio in this day")
    resultDF.show(10)

    var cpaMap = mutable.LinkedHashMap[String, Double]()
    for(row <- resultDF.collect()) {
      val ideaid = row.getAs[Int]("ideaid").toString
      val cpaRatio = row.getAs[Double]("cpa_ratio")
      cpaMap += (ideaid -> cpaRatio)
    }
    cpaMap

  }

  def getTargetK2(cpaMap: mutable.LinkedHashMap[String, Double], hour: String, ideaid: String, spark: SparkSession) = {
    // 每天12点之后，如果当天cpa过低（1.3），targetK -> 1.0
    // 每天12点之后，如果当天cpa过高（0.7）, targetK -> 0.7
    val cpaRatio = cpaMap.getOrElse[Double](ideaid, 0.0)
    val hourInt = hour.toInt
    var targetK = 0.95
    if (hourInt >= 12 && cpaMap.contains(ideaid) && cpaRatio >= 1.3) {
      targetK = 0.95
    } else if (hourInt >= 12 && cpaMap.contains(ideaid) && cpaRatio <= 0.7) {
      targetK = 0.95
    } else {
      targetK = 0.95
    }
    targetK
  }
}

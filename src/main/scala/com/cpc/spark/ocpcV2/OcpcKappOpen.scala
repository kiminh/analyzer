package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils._
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory

object OcpcKappOpen {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession.builder().appName(s"ocpc app open: $date, $hour").enableHiveSupport().getOrCreate()

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

    val openAppData = filterRepeatedUid(date, hour, 72, spark)
    openAppData.createOrReplaceTempView("open_app_data")
//    openAppData.write.mode("overwrite").saveAsTable("test.ocpc_check_app_20181227")

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
         |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1 and ocpc_log_dict['cpcBid']>0 and exptags not like "%cpcBid%") a
         |  left outer join
         |  (select searchid, label as label2 from open_app_data where $dtCondition and label=1) b on a.searchid = b.searchid
         |  left outer join
         |  (select searchid, label as label3 from dl_cpc.ml_cvr_feature_v2 where $dtCondition and label=1 group by searchid, label) c on a.searchid = c.searchid
         |group by ideaid,
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5),
         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5),
         |  ocpc_log_dict['cpagiven']
      """.stripMargin

    println(statSql)

    val realCvr3 = getIdeaidCvr3Ratio(date, hour, spark)


    val tablename = "dl_cpc.cpc_ocpc_v2_app_open_middle"
    val rawData = spark.sql(statSql)


    val data = rawData
      .join(realCvr3, Seq("ideaid"), "left_outer")
      .withColumn("cvr3_ratio", udfSqrt()(col("cvr_ratio")))
      .withColumn("cpa3", col("cpa3") * 1.0 / col("cvr3_ratio"))
      .withColumn("ratio3", col("ratio3") * 1.0 / col("cvr3_ratio"))
      .withColumn("cvr3Cnt", col("cvr3Cnt") * col("cvr3_ratio"))
      .select("ideaid", "k_ratio2", "k_ratio3", "cpagiven", "cpa2", "cpa3", "ratio2", "ratio3", "clickCnt", "cvr2Cnt", "cvr3Cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable(tablename)
    data
      .repartition(10).write.mode("overwrite").insertInto(tablename)

    val ratio2Data = getKWithRatioType(spark, tablename, "ratio2", date, hour)
    val ratio3Data = getKWithRatioType(spark, tablename, "ratio3", date, hour)

    val res = ratio2Data.join(ratio3Data, Seq("ideaid", "date", "hour"), "outer")
      .select("ideaid", "k_ratio2", "k_ratio3", "date", "hour")
//    res.write.mode("overwrite").saveAsTable("test.ocpc_v2_app_open_k")
    res
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_v2_app_open_k")

  }

  def filterRepeatedUid(date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
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
    val selectCondition1 = getTimeRangeSql3(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql2(date1, hour1, date, hour)

    val ctrData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(selectCondition1)
      .filter("isclick=1")
      .withColumn("date", col("dt"))
      .select("searchid", "timestamp", "uid", "ideaid", "date", "hour")

    val cvrData = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition2)
      .filter("label_sdk_dlapp=1 and label_type!=12")
      .withColumn("label", col("label_sdk_dlapp"))
      .select("searchid", "label", "date", "hour")


    val joinData = ctrData
      .join(cvrData.select("searchid", "label"), Seq("searchid"), "inner")
      .select("searchid", "timestamp", "uid", "ideaid", "label", "date", "hour")

    joinData.createOrReplaceTempView("join_table")

    val sqlRequest =
      s"""
         |SELECT
         |    t.searchid,
         |    t.timestamp,
         |    t.uid,
         |    t.ideaid,
         |    t.label,
         |    t.date,
         |    t.hour
         |FROM
         |    (SELECT
         |        searchid,
         |        timestamp,
         |        uid,
         |        ideaid,
         |        label,
         |        date,
         |        hour,
         |        row_number() over(partition by uid, ideaid order by timestamp) as seq
         |    FROM
         |        test.ocpc_join_data20181227) t
         |WHERE
         |    t.seq=1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getKWithRatioType(spark: SparkSession, tablename: String, ratioType: String, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and $ratioType is not null"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col(s"k_$ratioType"), col(s"$ratioType"), col("clickCnt")))
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
      // TODO 控制k值的增加上限
      // 每天12点之后，如果当天cpa过低（1.3），targetK -> 1.0
      // 每天12点之后，如果当天cpa过高（0.7）, targetK -> 0.7

      val targetK = getTargetK2(cpaMap, hour, ideaid, spark)
      val k = getResultK(targetK, coffList(0), coffList(1), spark)
      val cpaRatio = cpaMap.getOrElse(ideaid, 0.0)
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

  def getIdeaidCvr3Ratio(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition1 = getTimeRangeSql2(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest0 =
      s"""
         |SELECT
         |  ideaid,
         |  COUNT(distinct searchid) as total_cvr_cnt
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition1
         |AND
         |  label=1
         |GROUP BY ideaid
       """.stripMargin

    val rawData = spark.sql(sqlRequest0)

    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  b.label
         |FROM
         |  (select * from dl_cpc.ocpc_unionlog where $selectCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1) as a
         |LEFT JOIN
         |  (select searchid, label from dl_cpc.ml_cvr_feature_v2 where $selectCondition1 and label=1 group by searchid, label) as b
         |ON
         |  a.searchid=b.searchid
       """.stripMargin

    println(sqlRequest)
    val filteredData = spark
      .sql(sqlRequest)
      .groupBy("ideaid")
      .agg(sum(col("label")).alias("cvr_cnt"))
      .select("ideaid", "cvr_cnt")

    val resultDF = filteredData
      .join(rawData, Seq("ideaid"), "left_outer")
      .withColumn("cvr_ratio", col("total_cvr_cnt") * 1.0 / col("cvr_cnt"))
      .withColumn("cvr_ratio", when(col("cvr_ratio")<1, 1.0).otherwise(col("cvr_ratio")))
      .withColumn("cvr_ratio", when(col("cvr_ratio")>10, 10.0).otherwise(col("cvr_ratio")))


    resultDF

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
      .filter("label2=1 and label_type!=12")
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

    //    全量采用
    //    // 读取实验ideaid列表
    //    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    //    val data = spark.sparkContext.textFile(filename)
    //    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    rawRDD.foreach(println)
    //    val expIdeas = rawRDD.toDF("ideaid", "flag").distinct()

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
    var targetK = 0.97
    if (hourInt >= 12 && cpaMap.contains(ideaid) && cpaRatio >= 1.3) {
      targetK = 1.0
    } else if (hourInt >= 12 && cpaMap.contains(ideaid) && cpaRatio <= 0.7) {
      targetK = 0.9
    } else {
      targetK = 0.95
    }
    targetK
  }

  def getResultK(targetK: Double, x0: Double, x1: Double, spark: SparkSession) = {
    /*
    限制提高拟合目标值的情况下，k值变化过大的情况：
    maxK = 2 * originalK
    originalK为拟合目标值为0.95时得到的结果
     */
    val k = (targetK - x0) / x1
    val originalK = (0.95 - x0) / x1

    var resultK = k
    val maxK = originalK * 2
    if (k > maxK) {
      resultK = maxK
    } else {
      resultK = k
    }
    resultK

  }


}

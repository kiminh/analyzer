package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj._

object OcpcKnewV2 {
  /*
  OcpcK的升级：增加对二类电商的专门回归模型支持
   */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    // val onDuty = args(2).toInt

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

    val rawSql =
      s"""
         |select
         |    a.searchid,
         |    a.ideaid,
         |    a.ocpc_log_dict,
         |    a.price,
         |    a.isclick,
         |    a.exp_cvr,
         |    b.label_cvr1,
         |    b.label_cvr2,
         |    b.label_cvr3
         |from
         |    (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1 and ocpc_log_dict['cpcBid']>0 and exptags not like "%cpcBid%") a
         |    left outer join
         |    (select searchid, label2 as label_cvr1 from dl_cpc.ml_cvr_feature_v1 where $dtCondition and label_type!=12) b on a.searchid = b.searchid
         |    left outer join
         |    (select searchid, label as label_cvr2 from dl_cpc.ml_cvr_feature_v2 where $dtCondition and label=1 group by searchid, label) c on a.searchid = c.searchid
         |    left outer join
         |    (select searchid, 1 as label_cvr3 from dl_cpc.site_form_unionlog where $dtCondition group by searchid) d on a.searchid=d.searchid
       """.stripMargin
    println(rawSql)
    val dataRaw = spark.sql(rawSql)

    val dataRaw1 = filterHighPreCVR(dataRaw, "cvr1", date, hour, spark)
    val dataRaw2 = filterHighPreCVR(dataRaw, "cvr2", date, hour, spark)
    val dataRaw3 = filterHighPreCVR(dataRaw, "cvr3", date, hour, spark)

    val data1 = dataRaw1
      .withColumn("conversion_goal", lit(1))
    val data2 = dataRaw2
      .withColumn("conversion_goal", lit(2))
    val data3 = dataRaw3
      .withColumn("conversion_goal", lit(3))

    val data = data1.union(data2).union(data3)

//
//    val statSql =
//      s"""
//         |select
//         |  ideaid,
//         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio1,
//         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5) as k_ratio2,
//         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5) as k_ratio3,
//         |  ocpc_log_dict['cpagiven'] as cpagiven,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0)) as cpa1,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0)) as cpa3,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label1,0))/ocpc_log_dict['cpagiven'] as ratio1,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
//         |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0))/ocpc_log_dict['cpagiven'] as ratio3,
//         |  sum(isclick) clickCnt,
//         |  sum(COALESCE(label1,0)) cvr1Cnt,
//         |  sum(COALESCE(label2,0)) cvr2Cnt,
//         |  sum(COALESCE(label3,0)) cvr3Cnt
//         |from
//         |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1 and ocpc_log_dict['cpcBid']>0 and exptags not like "%cpcBid%") a
//         |  left outer join
//         |  (select searchid, label2 as label1 from dl_cpc.ml_cvr_feature_v1 where $dtCondition and label_type!=12) b on a.searchid = b.searchid
//         |  left outer join
//         |  (select searchid, label as label2 from dl_cpc.ml_cvr_feature_v2 where $dtCondition and label=1 group by searchid, label) c on a.searchid = c.searchid
//         |  left outer join
//         |  (select searchid, 1 as label3 from dl_cpc.site_form_unionlog where $dtCondition group by searchid) d on a.searchid=d.searchid
//         |group by
//         |  ideaid,
//         |  round(ocpc_log_dict['kvalue'] * 100.0 / 5),
//         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0 / 5),
//         |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cvr3cali'] * 100.0 / 5),
//         |  ocpc_log_dict['cpagiven']
//      """.stripMargin
//
//    println(statSql)

//    val realCvr3 = getIdeaidCvr3Ratio(date, hour, spark)


    val tablename = "test.cpc_ocpc_v2_middle_new_v2"
//    val rawData = spark.sql(statSql)


//    val data = rawData
//      .join(realCvr3, Seq("ideaid"), "left_outer")
//      .withColumn("cvr2_ratio", udfSqrt()(col("cvr_ratio")))
//      .withColumn("cpa2", col("cpa2") * 1.0 / col("cvr2_ratio"))
//      .withColumn("ratio2", col("ratio2") * 1.0 / col("cvr2_ratio"))
//      .withColumn("cvr2Cnt", col("cvr2Cnt") * col("cvr2_ratio"))
//      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3", "cpagiven", "cpa1", "cpa2", "cpa3", "ratio1", "ratio2", "ratio3", "clickCnt", "cvr1Cnt", "cvr2Cnt", "cvr3Cnt")
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))



    data.write.mode("overwrite").saveAsTable(tablename)
//    data
//      .repartition(10).write.mode("overwrite").insertInto(tablename)

    val ratio1Data = getKWithRatioType(spark, tablename, 1, date, hour)
    val ratio2Data = getKWithRatioType(spark, tablename, 2, date, hour)
    val ratio3Data = getKWithRatioType(spark, tablename, 3, date, hour)

    val res = ratio1Data
      .join(ratio2Data, Seq("ideaid", "date", "hour"), "outer")
      .join(ratio3Data, Seq("ideaid", "date", "hour"), "outer")
      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3", "date", "hour")

    res.write.mode("overwrite").saveAsTable("test.ocpc_v2_k_new")
//    res
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_v2_k_new")

  }

  def filterHighPreCVR(rawData: DataFrame, cvrType: String, date: String, hour: String, spark: SparkSession) = {
    val data = rawData
      .withColumn("label", col(s"label_$cvrType"))
      .selectExpr("searchid", "ideaid", "ocpc_log_dict", "price", "isclick", "exp_cvr", "label")

    val postCvrData = data
      .groupBy("ideaid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("label")).alias("conversion")
      )
      .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
      .select("ideaid", "post_cvr", "post_cvr_cali")

    val baseData = data
      .join(postCvrData, Seq("ideaid"), "inner")
      .filter(s"exp_cvr <= post_cvr_cali")
      .select("searchid", "ideaid", "ocpc_log_dict", "price", "isclick", "exp_cvr", "label")

    baseData.createOrReplaceTempView("base_table")

    val sqlRequest =
      s"""
         |select
         |    ideaid,
         |    round(ocpc_log_dict['kvalue'] * 100.0 / 5) as k_ratio,
         |    ocpc_log_dict['cpagiven'] as cpagiven,
         |    sum(if(isclick=1,price,0))/sum(COALESCE(label,0)) as cpa,
         |    sum(if(isclick=1,price,0))/sum(COALESCE(label,0))/ocpc_log_dict['cpagiven'] as ratio,
         |    sum(isclick) clickCnt,
         |    sum(COALESCE(label,0)) cvrCnt
         |from
         |    base_table
         |group by
         |    ideaid,
         |    round(ocpc_log_dict['kvalue'] * 100.0 / 5),
         |    ocpc_log_dict['cpagiven']
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))

    resultDF
  }

  def getKWithRatioType(spark: SparkSession, tablename: String, conversionGoal: Int, date: String, hour: String): Dataset[Row] = {

    val condition = s"`date` = '$date' and hour = '$hour' and conversion_goal=$conversionGoal"
    println("getKWithRatioType", condition)
    val res = spark.table(tablename).where(condition)
      .withColumn("str", concat_ws(" ", col("k_ratio"), col("ratio"), col("clickCnt")))
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
      //      val targetK = getTargetK2(cpaMap, hour, ideaid, spark)
      //      val k = (targetK - coffList(0)) / coffList(1)

      val targetK = getTargetK2(cpaMap, hour, ideaid, spark)
      val k = getResultK(targetK, coffList(0), coffList(1), spark)
      val cpaRatio = cpaMap.getOrElse(ideaid, 0.0)
      val realk: Double = k * 5.0 / 100.0
      println("ideaid " + ideaid, "coff " + coffList, "target k: " + k, "realk: " + realk, "targetK: " + targetK, "cpaRatio: " + cpaRatio)
      if (coffList(1)>0 && realk > 0) {
        resList.append((ideaid, realk, date, hour))
      }
    }
    var ratioType = "ratio1"
    if (conversionGoal == 1) {
      ratioType = "ratio1"
    } else if (conversionGoal == 2) {
      ratioType = "ratio2"
    } else {
      ratioType = "ratio3"
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

  //  def getCPAratio(date: String, hour: String, spark: SparkSession) = {
  //    import spark.implicits._
  //    // 取历史数据
  //    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
  //    val newDate = date + " " + hour
  //    val today = dateConverter.parse(newDate)
  //    val calendar = Calendar.getInstance
  //    calendar.setTime(today)
  //    calendar.add(Calendar.HOUR, -24)
  //    val yesterday = calendar.getTime
  //    val tmpDate = dateConverter.format(yesterday)
  //    val tmpDateValue = tmpDate.split(" ")
  //    val date1 = tmpDateValue(0)
  //    val hour1 = tmpDateValue(1)
  //    val selectCondition1 = getTimeRangeSql2(date1, hour1, date, hour)
  //    val selectCondition2 = getTimeRangeSql3(date1, hour1, date, hour)
  //
  //    val rawData1 = spark
  //      .table("dl_cpc.ocpc_unionlog")
  //      .where(s"`dt`='$date1'")
  //      .filter("isclick=1 and ocpc_log_dict['kvalue'] is not null")
  //
  //    val costData = rawData1
  //      .groupBy("ideaid")
  //      .agg(
  //        sum(col("price")).alias("cost"),
  //        avg(col("cpa_given")).alias("cpa_given"))
  //
  //    val rawData2 = spark
  //      .table("dl_cpc.ml_cvr_feature_v2")
  //      .where(s"`date`='$date1'")
  //      .filter("label=1")
  //      .select("ideaid", "label", "searchid")
  //      .distinct()
  //
  //    val cvrData = rawData2
  //      .groupBy("ideaid")
  //      .agg(sum(col("label")).alias("cvr_cnt"))
  //
  //    // 读取实验ideaid列表
  //    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
  //    val data = spark.sparkContext.textFile(filename)
  //    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
  //    rawRDD.foreach(println)
  //    val expIdeas = rawRDD.toDF("ideaid", "flag").distinct()
  //
  //    val resultDF = expIdeas
  //      .join(costData, Seq("ideaid"), "left_outer")
  //      .join(cvrData, Seq("ideaid"), "left_outer")
  //      .select("ideaid", "cpa_given", "cost", "cvr_cnt")
  //      .withColumn("cpa_real", col("cost") * 1.0 / col("cvr_cnt"))
  //      .withColumn("cpa_ratio", col("cpa_given") * 1.0 / col("cpa_real"))
  //
  //
  //    println("cpa ratio in past 24 hours")
  //    resultDF.show(10)
  //
  //    var cpaMap = mutable.LinkedHashMap[String, Double]()
  //    for(row <- resultDF.collect()) {
  //      val ideaid = row.getAs[Int]("ideaid").toString
  //      val cpaRatio = row.getAs[Double]("cpa_ratio")
  //      cpaMap += (ideaid -> cpaRatio)
  //    }
  //    cpaMap
  //
  //  }

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



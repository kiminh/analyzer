package com.cpc.spark.ocpcV3.HotTopicOcpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfSqrt
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.fitting.{ PolynomialCurveFitter, WeightedObservedPoints }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession, DataFrame}
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql3

import scala.collection.mutable

object OcpcRegression {
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
    } else if(media == "novel"){
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid = '80002819'"
    }

    // 中间表
    val middleData = getMiddleData(mediaSelection, version, hourCnt, date, hour, spark) // 返回热点段子ocpc广告的1,identifier, 2,k_ratio, 3,cpagiven, cpa, ratio(cpa/cpagiven), click_cnt, cvr_cnt
    val tablename = "dl_cpc.ocpc_regression_middle_hourly"
    val result = middleData
      .withColumn("conversion_goal", lit(1))
      .selectExpr("cast(identifier as string) identifier", "k_ratio", "cpagiven", "cpa", "ratio", "click_cnt", "cvr_cnt", "conversion_goal" )
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      result.write.mode("overwrite").saveAsTable("test.ocpc_regression_middle_hourly0301")
    result.write.mode("overwrite").insertInto( tablename )

    // 结果表
    val kvalue = getKWithRatio( middleData, date, hour, spark ) //返回结果为identifier,k_ratio
    val resultDF = kvalue
      .withColumn("conversion_goal", lit(1) )
      .select("identifier", "k_ratio", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//        resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_regression_hourly20190301")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_k_regression_hourly")

  }

  def getMiddleData(mediaSelection: String, version: String, hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 获取并关联数据
    /**
      * 返回ocpc_union_log_hourly中date日，hour时之前hourCnt小时内的identifier,k_ratio,cpagiven,cpa, ratio, click_cnt, cvr_cnt
      * */
    val ctrData = getCtrData(  mediaSelection, hourCnt, date, hour, spark ) //返回ocpc_union_log_hourly中date日，hour时之前hourCnt小时内的searchid, unitid, identifier, isclick, price, cpagiven, kvalue
    val cvrData = getCvr1Data( mediaSelection, hourCnt, date, hour, spark ) //返回ml_cvr_feature_v1中排除应用商城后的search_id及其对应的转化情况label2
    val rawData = ctrData  // identifier, kvalue, cpagiven, isclick, price, label
      .join( cvrData, Seq("searchid"), "left_outer" )
      .na.fill(0, Seq("label") )
      .select("identifier", "kvalue", "cpagiven", "isclick", "price", "label" )

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
    /***
      * 返回ocpc_union_log_hourly中date日，hour时之前hourCnt小时内的searchid,unitid, identifier,isclick, price, cpagiven,kvalue
      */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse( newDate )
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
         |  cast(ocpc_log_dict['kvalue']   as double) as kvalue
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
    /***
      * 返回ml_cvr_feature_v1中排除应用商城后的search_id及其对应的转化情况label2
      */
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

    val res = baseData  // Array
      .filter(s"ratio is not null")
      .withColumn("str", concat_ws(" ", col(s"k_ratio"), col("ratio"), col("click_cnt")) ) //concat_ws使用" "做为连接符，将后面3个字段的值连接成一行
      .groupBy("identifier" )
      .agg( collect_set("str").as("liststr" ) ) // 将数据按identifier分组后，对每组中"str"中的值去重后放入一个列表中，并重命名为liststr
      .select("identifier", "liststr").collect()
    //    rawData.write.mode("overwrite").saveAsTable("test.ocpc_check_regression20190103")

    //    val res = rawData.collect()
    val adclassMap = getAdclassMap(baseData, date, hour, spark)
    var resList = new mutable.ListBuffer[(String, Double)]()
    for ( row <- res ){  //每个identifier创建一个线性回归方程
      val identifier = row(0).toString
      val pointList  = row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].map( x => {
        val y = x.trim.split("\\s+")
        (y(0).toDouble, y(1).toDouble, y(2).toInt)
      } )
      val coffList = fitPoints( pointList.toList )
//      val targetK = 1.1
      val targetK = getTargetK(adclassMap, identifier)
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

  def fitPoints( pointsWithCount: List[(Double, Double, Int)] ): List[Double] = {
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

  def getTargetK(adclassMap: mutable.LinkedHashMap[String, Int], unitid: String) = {
    val adclass = adclassMap.getOrElse(unitid, 0) / 1000
    val adclassInt = adclass.toInt

    var result = 1.1
    if (adclassInt == 110110) {
      result = 0.7
    } else {
      result = 1.1
    }

    result
  }

  def getAdclassMap(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    /***
      * 返回ocpc_union_log_hourly中date日，hour时之前hourCnt小时内的searchid,unitid, identifier,isclick, price, cpagiven,kvalue
      */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse( newDate )
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  sum(ctr_cnt) as click
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002", "80002819")
         |GROUP BY unitid, adclass
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data.createOrReplaceTempView("base_data")

    val sqlRequest1 =
      s"""
         |SELECT
         |  t.unitid,
         |  t.adclass,
         |  t.click,
         |  t.seq
         |FROM
         |  (SELECT
         |      unitid,
         |      adclass,
         |      click,
         |      row_number() over(partition by unitid order by click desc) as seq
         |   FROM
         |       base_data) as t
         |WHERE
         |  t.seq=1
       """.stripMargin
    println(sqlRequest1)
    val unitidAdclass = spark.sql(sqlRequest1)

    unitidAdclass.show(10)
//    unitidAdclass.write.mode("overwrite").saveAsTable("test.sjq_unit_adclass_map")

    var adclassMap = mutable.LinkedHashMap[String, Int]()
    for(row <- unitidAdclass.collect()) {
      val unitid = row.getAs[Int]("unitid").toString
      val adclass = row.getAs[Int]("adclass")
      adclassMap += (unitid -> adclass)
    }
    adclassMap


  }

}

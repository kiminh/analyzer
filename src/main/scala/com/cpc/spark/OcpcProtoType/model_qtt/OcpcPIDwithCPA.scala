package com.cpc.spark.OcpcProtoType.model_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcPIDwithCPA {
  /*
  用于ocpc明投的相关代码
   */
  def main(args: Array[String]): Unit = {
    /*
    根据PID控制调整k值：
    1. 获得历史k值
    2. 获得历史cpa
    3. 根据给定cpa计算cpa_ratio
    4. 更新k值
     */
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    // bash: 2019-01-02 12 24 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val conversionGoal = args(3).toInt
    val version = args(4).toString
    val media = args(5).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt, conversionGoal=$conversionGoal, version=$version, media=$media")
    var mediaSelection = ""
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else if(media == "novel"){
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid = '80002819'"
    }

    // TODO 表名
    val prevTable = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version='$version'")

    val historyData = getHistory(mediaSelection, date, hour, spark)
    val result = calculateKwithConversionGoal(conversionGoal, hourInt, prevTable, historyData, date, hour, spark)

    val resultDF = result
        .withColumn("kvalue", col("k_value"))
        .select("identifier", "kvalue")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))
        .withColumn("method", lit("pid"))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pid_k_hourly")
//
//    resultDF
//      .repartition(10)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_k_model_hourly")


  }

  def calculateKwithConversionGoal(conversionGoal: Int, hourInt: Int, prevTable: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    按照给定的conversion_goal, hourInt，历史数据计算该conversion_goal下各个identifier最新的k值
    1. 获取cvr记录
    2. 获取历史k值
    3. 通过searchid关联，计算cpagiven与cpareal
    4. 计算cpa_ratio
    5. 根据cpa_ratio调整k值
     */
    val cvrData = getCVRdata(conversionGoal, hourInt, date, hour, spark)
    val kvalue = getHistoryK(historyData, prevTable, conversionGoal, date, hour, spark)
    val cpaHistory = getCPAhistory(historyData, cvrData, conversionGoal, date, hour, spark)
    val cpaRatio = calculateCPAratio(cpaHistory, date, hour, spark)
    val result = updateK(kvalue, cpaRatio, date, hour, spark)
    val resultDF = result.select("identifier", "k_value", "conversion_goal")
    resultDF
  }

  def getCVRdata(conversionGoal: Int, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
//    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
    val selectCondition = s"`date` >= '$date1'"
    // 抽取数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  ($selectCondition)
         |AND
         |  (cvr_goal = '$cvrGoal')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)

    resultDF
  }

  def getHistory(mediaSelection: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
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
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).filter(s"is_hidden = 0")
    resultDF
  }

  def getHistoryK(historyData: DataFrame, prevPb: DataFrame, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /**
      * 计算修正前的k基准值
      * case1：前6个小时有isclick=1的数据，统计这批数据的k均值作为基准值
      * case2：前6个小时没有isclick=1的数据，将前一个小时的数据作为基准值
      */
    // case1
    val case1 = historyData
      .filter("isclick=1")
      .groupBy("identifier")
      .agg(avg(col("kvalue")).alias("kvalue1"))
      .select("identifier", "kvalue1")

    // case2
    val case2 = prevPb
      .filter(s"conversion_goal = $conversionGoal")
      .withColumn("kvalue2", col("kvalue"))
      .select("identifier", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = case1
      .join(case2, Seq("identifier"), "outer")
      .select("identifier", "kvalue1", "kvalue2")
      .withColumn("kvalue", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))

    resultDF
  }

  def getCPAhistory(historyData: DataFrame, cvrRaw: DataFrame, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    计算cpa_history，分为cvr2和cvr3
    1. 数据关联
    2. 计算cost、cvr、cpa_history
     */

    // 数据关联
    val baseData = historyData
        .filter(s"isclick=1")
        .join(cvrRaw, Seq("searchid"), "left_outer")
        .select("searchid", "identifier", "price", "kvalue", "cpagiven", "isclick", "iscvr")

    val resultDF = baseData
        .groupBy("identifier")
        .agg(
          sum(col("price")).alias("cost"),
          avg(col("cpagiven")).alias("cpagiven"),
          sum(col("iscvr")).alias("cvrcnt")
        )
        .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
        .withColumn("conversion_goal", lit(conversionGoal))
        .select("identifier", "cost", "cpagiven", "cvrcnt", "cpa", "conversion_goal")

    resultDF
  }

  def calculateCPAratio(cpaHistory: DataFrame, date: String, hour: String, spark: SparkSession) = {
    cpaHistory.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cpagiven,
         |  cost,
         |  cvrcnt,
         |  cpa,
         |  (case when cpagiven is null then 1.0
         |        when cvrcnt is null or cvrcnt = 0 then 0.8
         |        when cvrcnt>0 then cpagiven * 1.0 / cpa
         |        else 1.0 end) as cpa_ratio,
         |   conversion_goal
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF

  }

  def updateK(kvalue: DataFrame, cpaRatio: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * case1: ratio < 0, t1
      * case2: ratio < 0.4, t2
      * case3: 0.4 <= ratio < 0.6, t3
      * case4: 0.6 <= ratio < 0.8, t4
      * case5: 0.8 <= ratio < 0.9, t5
      * case6: 0.9 <= ratio <= 1.1, t6
      * case7: 1.1 < ratio <= 1.2, t7
      * case8: 1.2 < ratio <= 1.4, t8
      * case9: 1.4 < ratio <= 1.6, t9
      * case10: ratio > 1.6, t10
      *
      * 上下限依然是0.2 到1.2
      */

    // 关联得到基础表
    val rawData = kvalue
      .join(cpaRatio, Seq("identifier"), "outer")
      .select("identifier", "cpa", "cpagiven", "cpa_ratio", "conversion_goal", "kvalue")

    val resultDF = rawData
      .withColumn("ratio_tag", udfSetRatioCase()(col("cpa_ratio")))
      .withColumn("updated_k", udfUpdateK()(col("ratio_tag"), col("kvalue")))
      .withColumn("k_value", col("updated_k"))


    resultDF

  }

}




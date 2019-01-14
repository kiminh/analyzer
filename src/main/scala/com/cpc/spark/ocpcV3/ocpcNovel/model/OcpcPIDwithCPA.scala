package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val result = calculateKv2(date, hour, spark)
//    result.write.mode("overwrite").saveAsTable("test.ocpc_novel_k_value_table")
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_novel_k_value_table")


  }

  /*******************************************************************************/
  def calculateKv2(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
    baseData.show(10)
    val historyData = getHistoryData(date, hour, 6, spark)
    println("################# historyData ####################")
    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
    avgK.show(10)
    val cpaRatio = getCPAratio(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
    newK.show(10)
    newK
  }

  def getBaseTable(endDate: String, hour: String, spark: SparkSession) :DataFrame ={
    // 计算日期周期
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateConverter.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = dateConverter.format(dt)
    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)

    // 累积计算最近一周数据
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE $selectCondition
         |  and media_appsid in ("80001098", "80001292")
         |GROUP BY unitid, adclass
       """.stripMargin
    println(sqlRequest)
    val baseData = spark.sql(sqlRequest)

    baseData

  }

  def getHistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

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
         |  adclass,
         |  isshow,
         |  isclick,
         |  price,
         |  ocpc_log,
         |  hour
         |FROM
         |  dl_cpc.ocpcv3_unionlog_label_hourly
         |WHERE
         |  $selectCondition
         |and
         |media_appsid in ('80001098', '80001292')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }


  def getAvgK(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 计算修正前的k基准值
      * case1：前6个小时有isclick=1的数据，统计这批数据的k均值作为基准值
      * case2：前6个小时没有isclick=1的数据，将前一个小时的数据作为基准值
      * case3: 在主表（7*24）中存在，但是不属于前两种情况的，初始值0.694
      */

    historyData
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .createOrReplaceTempView("raw_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  ocpc_log,
         |  ocpc_log_dict['kvalue'] as kvalue,
         |  hour
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest2)
    val rawData = spark.sql(sqlRequest2)

    // case1
    val case1 = rawData
      .filter("isclick=1")
      .groupBy("unitid", "adclass")
      .agg(avg(col("kvalue")).alias("kvalue1")).select("unitid", "adclass", "kvalue1")

    // case2
    // table name: dl_cpc.ocpcv3_novel_pb_hourly
    // TODO 去重
    val case2 = spark
      .table("test.ocpcv3_novel_pb_v1_hourly")
      .withColumn("kvalue2", col("kvalue"))
      .select("unitid", "kvalue2")
      .distinct()
//    val case2 = spark
//      .table("test.ocpcv3_novel_pb_v1_hourly")
//      .withColumn("kvalue2", col("kvalue"))
//      .groupBy("unitid")
//      .agg(avg(col("kvalue2")).alias("kvalue2"))
//      .select("unitid", "kvalue2")
//      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = baseData
      .join(case1, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue1")
      .join(case2, Seq("unitid"), "left_outer")
      .select("unitid", "adclass", "kvalue1", "kvalue2")
      .withColumn("kvalue", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_pid_avgk_hourly")

    resultDF.show(10)
    resultDF

  }

  def getCPAratio(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 计算前6个小时每个广告创意的cpa_given/cpa_real的比值
      * case1：hourly_ctr_cnt<10，可能出价过低，需要提高k值，所以比值应该大于1
      * case2：hourly_ctr_cnt>=10但是没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3：hourly_ctr_cnt>=10且有cvr_cnt，按照定义计算比值即可
      */

    // 获得cpa_given
    val cpaGiven = spark
      .table("dl_cpc.ocpcv3_novel_cpa_history_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .withColumn("cpa_given", col("cpa_history"))
      .select("unitid", "cpa_given", "conversion_goal")

    val cvr1Data=getCvr1HistoryData(date, hour, 6, spark)
      .groupBy("unitid", "adclass")
      .agg(sum(col("cvr1cnt")).alias("cvr1cnt"))
    val cvr2Data=getCvr2HistoryData(date, hour, 6, spark)
      .groupBy("unitid", "adclass")
      .agg(sum(col("cvr2cnt")).alias("cvr2cnt"))
    // 按ideaid和adclass统计每一个广告创意的数据
    val rawData = historyData
      .withColumn("cost",
        when(col("isclick")===1,col("price")).otherwise(0))
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("cost")).alias("total_cost"),
        sum(col("isclick")).alias("ctr_cnt"))
      .select("unitid", "adclass", "total_cost", "ctr_cnt")
      .join(cvr1Data,Seq("unitid", "adclass"), "left_outer")
      .join(cvr2Data,Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "total_cost", "ctr_cnt", "cvr1cnt", "cvr2cnt")

    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("unitid"), "left_outer")
      .select("unitid", "adclass", "cpa_given", "conversion_goal")
      .join(rawData, Seq("unitid", "adclass"), "left_outer")
      .withColumn("cvr_cnt", when(col("conversion_goal")===2, col("cvr2cnt")).otherwise(col("cvr1cnt")))
      .select("unitid", "adclass", "cpa_given", "conversion_goal", "total_cost", "ctr_cnt", "cvr_cnt")
      .filter("cpa_given is not null and total_cost>0")

    joinData.createOrReplaceTempView("join_table")


    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  conversion_goal,
         |  cpa_given,
         |  total_cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  (case when cvr_cnt=0 or cvr_cnt is null then 0.8
         |        else cpa_given * cvr_cnt * 1.0 / total_cost end) as cpa_ratio
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatio = spark.sql(sqlRequest)
//    cpaRatio.write.mode("overwrite").saveAsTable("test.ocpcv3_pid_cparatio_hourly")

    cpaRatio

  }

  def updateKv2(baseData: DataFrame, kValue: DataFrame, cpaRatio: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * case1：0.9 <= cpa_ratio <= 1.1，k基准值
      * case2：0.8 <= cpa_ratio < 0.9，k / 1.1
      * case2：1.1 < cpa_ratio <= 1.2，k * 1.1
      * case3：0.6 <= cpa_ratio < 0.8，k / 1.2
      * case3：1.2 < cpa_ratio <= 1.4，k * 1.2
      * case4：0.4 <= cpa_ratio < 0.6，k / 1.4
      * case5：1.4 < cpa_ratio <= 1.6，k * 1.4
      * case6：cpa_ratio < 0.4，k / 1.6
      * case7：cpa_ratio > 1.6，k * 1.6
      *
      * 上下限依然是0.2 到1.2
      */

    // 关联得到基础表
    val rawData = baseData
      .join(kValue, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue")
      .join(cpaRatio, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue", "cpa_ratio", "conversion_goal")
      .withColumn("ratio_tag", udfSetRatioCase()(col("cpa_ratio")))
      .withColumn("updated_k", udfUpdateK()(col("ratio_tag"), col("kvalue")))


    val resultDF = rawData
      .select("unitid", "adclass", "updated_k", "conversion_goal")
      .withColumn("k_value", col("updated_k"))
      .select("unitid", "adclass", "k_value", "updated_k", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }


}
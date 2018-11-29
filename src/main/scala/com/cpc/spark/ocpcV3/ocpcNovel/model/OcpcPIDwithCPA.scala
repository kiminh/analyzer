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
    val onDuty = args(2).toInt // onDuty=1表示部署模型，onDuty!=1表示测试新代码

    // TODO ideaid与userid的名称
    if (onDuty == 1) {
      val result = calculateKv3(date, hour, spark)
      result.write.mode("overwrite").saveAsTable("test.ocpc_novel_k_value_table")
    } else {
      println("############## entering test stage ###################")
      val testKstrat = calculateKv3(date, hour, spark)
    }

  }

  /*******************************************************************************/
  def calculateKv2(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
//    baseData.show(10)
    val historyData = getHistoryData(date, hour, 6, spark)
    println("################# historyData ####################")
//    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
//    avgK.show(10)
    val cpaRatio = getCPAratio(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
//    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
//    newK.show(10)
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
    // table name for previous calculation: test.new_pb_ocpc_with_pcvr
    // todo: dl_cpc.new_pb_ocpc_novel_with_pcvr
    val case2 = spark
      .table("dl_cpc.new_pb_ocpc_novel_with_pcvr")
      .filter("media_appsid in ('80001098', '80001292')")
      .withColumn("kvalue2", col("k_value"))
      .select("unitid", "adclass", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = baseData
      .join(case1, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue1")
      .join(case2, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue1", "kvalue2")
      .withColumn("kvalue_new", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))
      .withColumn("kvalue", when(col("kvalue_new").isNull, 0.694).otherwise(col("kvalue_new")))

    resultDF.show(10)
    resultDF

  }

  def getCPAratio(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame ={
    // TODO case
    /**
      * 计算前6个小时每个广告创意的cpa_given/cpa_real的比值
      * case1：hourly_ctr_cnt<10，可能出价过低，需要提高k值，所以比值应该大于1
      * case2：hourly_ctr_cnt>=10但是没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3：hourly_ctr_cnt>=10且有cvr_cnt，按照定义计算比值即可
      */

    // 获得cpa_given
    val cpaGiven = spark.table("test.ocpc_novel_idea_update_time_" + hour).select("unitid", "cpa_given")

    val Cvr1data=getCvr1HistoryData(date, hour, 24, spark)
    // 按ideaid和adclass统计每一个广告创意的数据
    val rawData = historyData
      .withColumn("cost",
        when(col("isclick")===1,col("price")).otherwise(0))
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("cost")).alias("total_cost"),
        sum(col("isclick")).alias("ctr_cnt"))
      .join(Cvr1data,Seq("unitid","adclass"),"left_outer")


    // 计算单个小时的ctr_cnt和cvr_cnt
    val singleHour = historyData
      .filter(s"hour='$hour'")
      .groupBy("unitid", "adclass").agg(sum("isclick").alias("hourly_ctr_cnt"), sum(col("iscvr")).alias("hourly_cvr_cnt"))
      .select("unitid", "adclass", "hourly_ctr_cnt", "hourly_cvr_cnt")

    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("unitid"), "left_outer")
      .select("unitid", "adclass", "cpa_given")
      .join(rawData, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt")
      .join(singleHour, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cpa_given", "total_cost", "ctr_cnt", "cvr_cnt", "hourly_ctr_cnt", "hourly_cvr_cnt")

    joinData.createOrReplaceTempView("join_table")


    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  cpa_given,
         |  total_cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  (case when cpa_given is null then 1.0
         |        when '$hour'>'05' and (hourly_ctr_cnt<5 or hourly_ctr_cnt is null) then 1.2
         |        when hourly_ctr_cnt>=10 and (cvr_cnt=0 or cvr_cnt is null) then 0.8
         |        when cvr_cnt>0 then cpa_given * cvr_cnt * 1.0 / total_cost
         |        else 1.0 end) as cpa_ratio
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatio = spark.sql(sqlRequest)

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
    // todo 删除临时表
    baseData.write.mode("overwrite").saveAsTable("test.ocpc_base_table")
    val rawData = baseData
      .join(kValue, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue")
      .join(cpaRatio, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "kvalue", "cpa_ratio")
      .withColumn("ratio_tag", udfSetRatioCase()(col("cpa_ratio")))
      .withColumn("updated_k", udfUpdateK()(col("ratio_tag"), col("kvalue")))

    rawData.createOrReplaceTempView("raw_table")

//    rawData
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_k_value_raw_table")

    val cvr3Data = getActivationData(date, hour, spark)

    val resultDF = rawData
      .select("unitid", "adclass", "updated_k")
      .withColumn("k_value", when(col("updated_k").isNull, 0.694).otherwise(col("updated_k")))
      .select("unitid", "adclass", "k_value", "updated_k")

    resultDF
  }

  /*******************************************************************/
  // TODO: 下一阶段优化目标，考虑24小时的cpa表现，按权重计算
  def calculateKv3(date: String, hour: String, spark: SparkSession) :DataFrame ={
    /**
      * 计算新版k值
      * 基于前6个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      */

    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
//    baseData.show(10)
    val historyData = getHistoryData(date, hour, 24, spark)
    println("################# historyData ####################")
//    historyData.show(10)
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
//    val avgK = getAvgKV3(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
//    avgK.show(10)
    val cpaRatio = getCPAratioV3(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
//    cpaRatio.show(10)
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
//    newK.show(10)
    newK

  }

  def getCPAratioV3(baseData: DataFrame, historyData: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 按照权值在24h的时间窗口内计算cpa_ratio
      * case1：hourly_ctr_cnt<10，可能出价过低，需要提高k值，所以比值应该大于1
      * case2：hourly_ctr_cnt>=10但是没有cvr_cnt，可能出价过高，需要降低k值，所以比值应该小于1
      * case3：hourly_ctr_cnt>=10且有cvr_cnt，按照定义计算比值即可
      */

    // 按ideaid和adclass统计每一个广告创意的加权数据
    val Cvr1data=getCvr1HistoryData(date, hour, 24, spark)

    val rawData = historyData
      .withColumn("cost",
        when(col("isclick")===1, col("price")).otherwise(0))
      .groupBy("unitid", "adclass", "hour")
      .agg(
        sum(col("cost")).alias("cost"),
        sum(col("isclick")).alias("ctr_cnt"))
      .join(Cvr1data, Seq("unitid", "adclass", "hour"), "left_outer")
      .withColumn("cvr_cnt", col("cvr1cnt"))
      .withColumn("weight", udfCalculateWeightByHour(hour)(col("hour")))
      .withColumn("weighted_cost", col("cost") * col("weight"))
      .withColumn("weighted_cvr_cnt", col("cvr_cnt") * col("weight"))
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("weighted_cost")).alias("total_cost"),
        sum(col("weighted_cvr_cnt")).alias("cvr_cnt"))

    // 获得cpa_given
    val cpaGiven = spark.table("test.ocpc_novel_idea_update_time_" + hour).select("unitid", "cpa_given")

    // 计算单个小时的ctr_cnt和cvr_cnt
    val singleHour = historyData
      .filter(s"hour='$hour'")
      .groupBy("unitid", "adclass").agg(sum("isclick").alias("hourly_ctr_cnt"), sum(col("iscvr")).alias("hourly_cvr_cnt"))
      .select("unitid", "adclass", "hourly_ctr_cnt", "hourly_cvr_cnt")


    // 计算cpa_ratio
    val joinData = baseData
      .join(cpaGiven, Seq("unitid"), "left_outer")
      .select("unitid", "adclass", "cpa_given")
      .join(rawData, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cpa_given", "total_cost", "cvr_cnt")
      .join(singleHour, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cpa_given", "total_cost", "cvr_cnt", "hourly_ctr_cnt", "hourly_cvr_cnt")


    joinData.createOrReplaceTempView("join_table")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  cpa_given,
         |  total_cost,
         |  cvr_cnt,
         |  (case when cpa_given is null then 1.0
         |        when cvr_cnt=0 or cvr_cnt is null then 0.8
         |        when cvr_cnt>0 then cpa_given * cvr_cnt * 1.0 / total_cost
         |        else 1.0 end) as cpa_ratio_cvr2
         |FROM
         |  join_table
       """.stripMargin
    println(sqlRequest)
    val cpaRatioCvr2 = spark.sql(sqlRequest)

    val cpaRatioCvr3 = getAPIcvr3V3(singleHour, date, hour, spark)

    val cpaRatio = cpaRatioCvr2
      .join(cpaRatioCvr3, Seq("unitid", "adclass"), "left_outer")
      .withColumn("cpa_ratio", when(col("flag").isNotNull, col("cpa_ratio_cvr3")).otherwise(col("cpa_ratio_cvr2")))

//    cpaRatio
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_check_cpa_ratio_hourly")

    cpaRatio

  }

  def getActivationData(date: String, hour: String, spark: SparkSession) = {

    val resultDF = spark
      .table("test.ocpc_novel_idea_update_time_" + hour)
      .filter("conversion_goal=2")
      .withColumn("flag", lit(1))
      .select("unitid", "cpa_given", "flag")


    resultDF
  }

  //TODO 给api回传模型做反馈机制
  def getAPIcvr3V3(singleHour: DataFrame, date: String, hour: String, spark: SparkSession) :DataFrame = {
    val cvr3List = getActivationData(date, hour, spark)

    val cvr3Data = getActDataNovel(date, hour, 24, spark)

    val rawData = cvr3Data
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("cost")).alias("cvr3_cost"),
        sum(col("cvr2cnt")).alias("cvr3_cvr_cnt"))
      .select("unitid", "adclass", "cvr3_cost", "cvr3_cvr_cnt")



    val historyData = getCompleteHistoryData(date, hour, 24, spark)
    val costData = historyData
      .groupBy("unitid", "adclass")
      .agg(sum(col("cost")).alias("cost"))
      .select("unitid", "adclass", "cost")

    // 计算单个小时的ctr_cnt和cvr_cnt
    // todo  删除临时表
    singleHour.write.mode("overwrite").saveAsTable("test.test_ocpc_cvr3_debug_singlehour")

    val data = cvr3List
      .join(costData, Seq("unitid"), "left_outer")
      .select("unitid", "adclass", "cost", "cpa_given", "flag")
      .join(rawData, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cost", "cvr3_cost", "cvr3_cvr_cnt", "cpa_given", "flag")
      .join(singleHour, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "cost", "cvr3_cost", "cvr3_cvr_cnt", "cpa_given", "flag", "hourly_ctr_cnt")



    data.createOrReplaceTempView("data_table")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  cost,
         |  cpa_given,
         |  cvr3_cvr_cnt,
         |  hourly_ctr_cnt,
         |  flag,
         |  (case when cpa_given is null then 1.0
         |        when cvr3_cvr_cnt=0 or cvr3_cvr_cnt is null then 0.8
         |        when cvr3_cvr_cnt>0 then cpa_given * cvr3_cvr_cnt * 1.0 / cost
         |        else 1.0 end) as cpa_ratio_cvr3
         |FROM
         |  data_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    val finalDF = resultDF.select("unitid", "adclass", "flag", "cvr3_cvr_cnt", "cpa_ratio_cvr3")
    finalDF
  }

}